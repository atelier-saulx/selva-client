import { Aggregate, GetOptions, Filter } from '../types'
import { SelvaClient } from '../..'

import checkAllowed from './checkAllowed'
import validateFilter from './filter'

import { get } from '..'
import { addExtraQuery, ExtraQueries } from '.'
import validateFind from './find'
import validateSort from './sort'
import { isTraverseByType, isTraverseOptions } from '../utils'

// TODO: more concurrency for fetching
async function evaluateTextSearch(
  filters: Filter[],
  language: string,
  logicalOperator: 'and' | 'or',
  nested: boolean = false
): Promise<string[]> {
  const textSearches = (
    await Promise.all(
      filters.map(async (f, i) => {
        if ((!nested && f.$and) || (i && f.$and)) {
          return evaluateTextSearch([f, f.$and], language, 'and', true)
        } else if ((!nested && f.$or) || (i && f.$or)) {
          return evaluateTextSearch([f, f.$or], language, 'or', true)
        } else if (f.$operator === 'textSearch') {
          // const resp = await fetch('http://localhost:33333/get', {
          //   method: 'POST',
          //   headers: {
          //     'content-type': 'application/json',
          //   },
          //   body: JSON.stringify({
          //     $searchString: f.$value,
          //     $field: f.$field,
          //     $language: language, // FIXME
          //   }),
          // })

          // const ids = await resp.json()
          // return ids
          return []
        } else {
          return null
        }
      })
    )
  ).filter((ids) => !!ids)

  if (textSearches.length) {
    const results: string[][] = await Promise.all(
      textSearches.map(async (f: Filter | string[]) => {
        // TODO: replace hard coded url and port
        //if (Array.isArray(f)) {
        //} else {
        //  const resp = await fetch('http://localhost:33333/get', {
        //    method: 'POST',
        //    headers: {
        //      'content-type': 'application/json',
        //    },
        //    body: JSON.stringify({
        //      $searchString: f.$value,
        //      $field: f.$field,
        //      $language: language, // FIXME
        //    }),
        //  })

        //  const ids = await resp.json()
        //  return ids
        //}
        return []
      })
    )

    if (logicalOperator === 'or') {
      // set union
      const set: Set<string> = new Set()
      for (const resultSet of results) {
        for (const id of resultSet) {
          set.add(id)
        }
      }

      return [...set.values()]
    } else {
      // set intersection
      const resultSet: Set<string> = new Set()
      for (const id of results[0]) {
        resultSet.add(id)
      }

      for (let i = 1; i < results.length; i++) {
        const set = new Set(...results[i])
        for (const x of resultSet.values()) {
          if (!set.has(x)) {
            resultSet.delete(x)
          }
        }
      }

      return [...resultSet.values()]
    }
  }

  return []
}

export default async function validateAggregate(
  extraQueries: ExtraQueries,
  parentProp: GetOptions,
  client: SelvaClient,
  find: Aggregate,
  path: string
): Promise<void> {
  const err = (mainMsg?: string): never => {
    if (!mainMsg) {
      mainMsg = 'Unsupported type in operator $aggregate'
    }

    throw new Error(
      `${mainMsg} for ${path}.$aggregate. Required type object with the following properties:
        {
          $traverse: 'descendants' | 'ancestors' | string | string[] (optional)
          $recursive: boolean (optional)
          $filter: FilterOptions | FilterOptions[] (and by default) (optional)


        FilterOptions:
          {
            $operator: '=' | '!=' | '>' | '<' | '..'
            $field: string
            $value: string | number | (string | number)[]
            $and: FilterOptions (adds an additional condition) (optional)
            $or: FilterOptions (adds optional condition) (optional)
          }
        `
    )
  }

  const allowed = checkAllowed(
    find,
    new Set([
      '$function',
      '$traverse',
      '$recursive',
      '$filter',
      '$db',
      '$find',
      '$sort',
      '$limit',
      '$offset',
    ])
  )
  if (allowed !== true) {
    err(`Unsupported operator or field ${allowed}`)
  }

  if (find.$traverse) {
    const traverse = find.$traverse
    if (isTraverseOptions(traverse)) {
      const result = await get(client, {
        $includeMeta: true,
        $db: traverse.$db,
        $id: traverse.$id,
        traverse: {
          $field: traverse.$field,
        },
      })
      const meta = result.$meta
      delete result.$meta

      addExtraQuery(extraQueries, {
        $db: traverse.$db,
        type: 'traverse',
        meta: meta,
        value: result.traverse,
        path: path + '.$find.$traverse',
      })
    } else if (isTraverseByType(traverse)) {
      // FIXME: needs specific validation?
    } else if (
      typeof find.$traverse !== 'string' &&
      !Array.isArray(find.$traverse)
    ) {
      err(`Unsupported type for $traverse ${find.$traverse}`)
    }
  }

  if (find.$recursive && typeof find.$recursive !== 'boolean') {
    err(`Invalid value for $recursive ${find.$recursive}`)
  }

  if (find.$filter) {
    let filterAry: Filter[] = []
    if (Array.isArray(find.$filter)) {
      for (const filter of find.$filter) {
        validateFilter(client, filter, path + '.$find')
      }

      filterAry = find.$filter
    } else {
      validateFilter(client, find.$filter, path + '.$find')
      filterAry = [find.$filter]
    }

    if (find.$find) {
      validateFind(extraQueries, parentProp, client, find.$find, path)
    }

    // // FIXME: proper $language param
    // const textSearchIds = await evaluateTextSearch(
    //   filterAry,
    //   'en',
    //   'and',
    //   false
    // )
    const textSearchIds = []

    if (textSearchIds && textSearchIds.length) {
      // if there already is a traverse, we need to get the intersection with textSearchIds
      addExtraQuery(extraQueries, {
        type: 'text_search',
        $db: 'default', // TODO: fix this
        meta: null,
        path: path + '.$find.$traverse',
        value: textSearchIds,
      })
    }
  }

  if (find.$sort) {
    validateSort(client, find.$sort, path)
  }
}
