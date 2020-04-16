import * as redis from '../redis'
import { Id, Schema } from '~selva/schema/index'
import { getTypeFromId } from '../typeIdMapping'
import { GetResult, GetItem } from '~selva/get/types'
import { setNestedResult } from './nestedFields'
import getByType from './getByType'
import { ensureArray, splitString, isArray } from '../util'
import * as logger from '../logger'
import getWithField from 'lua/src/get/field'
import { GetFieldFn } from './types'

function getAncestorsByType(
  types: string[],
  ancestorsWithScores: string[]
): Record<string, Id[]> {
  let results: Record<string, Id[]> = {}
  for (const itemType of types) {
    results[itemType] = []
  }

  for (let i = ancestorsWithScores.length - 2; i >= 0; i -= 2) {
    const ancestorType = getTypeFromId(ancestorsWithScores[i])
    if (results[ancestorType]) {
      const matches = results[ancestorType]
      matches[matches.length] = ancestorsWithScores[i]
    }
  }

  return results
}

function prepareRequiredFieldSegments(fields: string[]): string[][] {
  const requiredFields: string[][] = []
  for (let i = 0; i < fields.length; i++) {
    const r = fields[i]
    const segments: string[] = splitString(r, '.')
    requiredFields[i] = segments
  }

  return requiredFields
}

type Query = {
  ids: Record<string, true>
  fields: Record<string, true>
}

function setFromAncestors(
  getField: GetFieldFn,
  result: GetResult,
  schema: Schema,
  id: Id,
  field: string,
  language?: string,
  version?: string,
  includeMeta?: boolean,
  fieldFrom?: string | string[],
  merge?: boolean,
  tryAncestorCondition?: (ancestor: Id) => boolean,
  acceptAncestorCondition?: (result: any) => boolean,
  ancestorsWithScores?: Id[],
  props?: GetItem
): boolean {
  const parents = redis.smembers(id + '.parents')

  if (!ancestorsWithScores) {
    ancestorsWithScores = redis.zrangeWithScores(id + '.ancestors')
  }

  const ancestorDepthMap: Record<Id, number> = {}

  for (let i = 0; i < ancestorsWithScores.length; i += 2) {
    ancestorDepthMap[ancestorsWithScores[i]] =
      tonumber(ancestorsWithScores[i + 1]) || 0
  }

  let validParents: Id[] = []
  for (let i = 0; i < parents.length; i++) {
    if (ancestorDepthMap[parents[i]]) {
      validParents[validParents.length] = parents[i]
    }
  }

  // we want to check parents from deepest to lowest depth
  table.sort(validParents, (a, b) => {
    return ancestorDepthMap[a] > ancestorDepthMap[b]
  })

  while (validParents.length > 0) {
    const next: Id[] = []
    for (const parent of validParents) {
      if (includeMeta) {
        result.$meta.inherit[id].ids[parent] = true
      }

      if (
        !tryAncestorCondition ||
        (tryAncestorCondition && tryAncestorCondition(parent))
      ) {
        if (fieldFrom && fieldFrom.length > 0) {
          if (
            getWithField(
              result,
              schema,
              parent,
              field,
              fieldFrom,
              language,
              version,
              includeMeta
            )
          ) {
            return true
          }
        } else if (field === '') {
          const intermediateResult = !acceptAncestorCondition ? result : {}
          getField(
            props || {},
            schema,
            intermediateResult,
            parent,
            '',
            language,
            version,
            //includeMeta,
            false, // FIXME: maybe need to support this here too?
            '$inherit'
          )

          if (!acceptAncestorCondition) {
            return true
          }

          if (acceptAncestorCondition(intermediateResult)) {
            for (const k in intermediateResult) {
              result[k] = intermediateResult[k]
            }
            return true
          }
        } else {
          if (
            getByType(
              result,
              schema,
              parent,
              field,
              language,
              version,
              includeMeta,
              merge
            )
          ) {
            return true
          }
        }
      }

      const parentsOfParents = redis.smembers(parent + '.parents')
      for (const parentOfParents of parentsOfParents) {
        if (ancestorDepthMap[parentOfParents]) {
          next[next.length] = parentOfParents
        }
      }
    }

    validParents = next
  }

  return false
}

function getName(id: Id): string {
  return redis.hget(id, 'name')
}

function inheritItem(
  getField: GetFieldFn,
  props: GetItem,
  schema: Schema,
  result: GetResult,
  id: Id,
  field: string,
  item: string[],
  required: string[],
  language?: string,
  version?: string,
  includeMeta?: boolean
) {
  const requiredFields: string[][] = prepareRequiredFieldSegments(required)

  const ancestorsWithScores = redis.zrangeWithScores(id + '.ancestors')
  const len = ancestorsWithScores.length
  if (len === 0) {
    setNestedResult(result, field, {})
    return
  }

  const ancestorsByType = getAncestorsByType(item, ancestorsWithScores)

  const intermediateResult = {}
  for (const itemType of item) {
    const matches = ancestorsByType[itemType]
    if (matches.length === 1) {
      const intermediateResult = {}
      getField(
        props,
        schema,
        intermediateResult,
        matches[0],
        '',
        language,
        version,
        // includeMeta, // FIXME: do we need to support this? don't think so
        false,
        '$inherit'
      )
      setNestedResult(result, field, intermediateResult)
      return
    } else if (matches.length > 1) {
      setFromAncestors(
        getField,
        intermediateResult,
        schema,
        id,
        '',
        language,
        version,
        includeMeta,
        '',
        false,
        (ancestor: Id) => {
          for (const match of matches) {
            if (match === ancestor) {
              return true
            }
          }

          return false
        },
        (result: any) => {
          if (required.length === 0) {
            return true
          }

          for (const requiredField of requiredFields) {
            let prop: any = result
            for (const segment of requiredField) {
              prop = prop[segment]
              if (!prop) {
                return false
              }
            }
          }

          return true
        },
        ancestorsWithScores,
        props || {}
      )

      setNestedResult(result, field, intermediateResult)
      return
    }
  }

  // set empty result
  setNestedResult(result, field, {})
}

export default function inherit(
  getField: GetFieldFn,
  props: GetItem,
  schema: Schema,
  result: GetResult,
  id: Id,
  field: string,
  language?: string,
  version?: string,
  includeMeta?: boolean,
  fieldFrom?: string | string[]
) {
  logger.info(`INHERIT DAT FIELD ${field}`, result, includeMeta)

  // add from where it inherited and make a descendants there
  // how to check if descandents in it checl if in acnestors

  const inherit = props.$inherit
  if (inherit) {
    if (inherit === true) {
      return setFromAncestors(
        getField,
        result,
        schema,
        id,
        field,
        language,
        version,
        includeMeta,
        fieldFrom,
        true
      )
    } else if (inherit.$type) {
      const types: string[] = ensureArray(inherit.$type)
      return setFromAncestors(
        getField,
        result,
        schema,
        id,
        field,
        language,
        version,
        includeMeta,
        fieldFrom,
        inherit.$merge !== undefined ? inherit.$merge : true,
        (ancestor: Id) => {
          for (const type of types) {
            if (type === getTypeFromId(ancestor)) {
              return true
            }
          }

          return false
        }
      )
    } else if (inherit.$name) {
      const names: string[] = ensureArray(inherit.$name)
      return setFromAncestors(
        getField,
        result,
        schema,
        id,
        field,
        language,
        version,
        includeMeta,
        fieldFrom,
        inherit.$merge !== undefined ? inherit.$merge : true,
        (ancestor: Id) => {
          for (const name of names) {
            if (name === getName(ancestor)) {
              return true
            }
          }

          return false
        }
      )
    } else if (inherit.$item) {
      logger.info('INHERITING with $item')
      inheritItem(
        getField,
        props,
        schema,
        result,
        id,
        field,
        ensureArray(inherit.$item),
        ensureArray(inherit.$required),
        language,
        version,
        includeMeta
      )
    } else if (inherit.$merge !== undefined) {
      // if only merge specified, same as inherit: true with merge off
      return setFromAncestors(
        getField,
        result,
        schema,
        id,
        field,
        language,
        version,
        includeMeta,
        fieldFrom,
        inherit.$merge
      )
    }

    return false
  }
}
