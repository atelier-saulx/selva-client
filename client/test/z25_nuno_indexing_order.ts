import test from 'ava'
import { connect } from '../src/index'
import { start } from '@saulx/selva-server'
import getPort from 'get-port'
import { wait } from './assertions'

let srv
let port: number

test.before(async (t) => {
  port = await getPort()
  srv = await start({
    port,
    selvaOptions: [
      'FIND_INDICES_MAX',
      '10',
      'FIND_INDEXING_INTERVAL',
      '1000',
      'FIND_INDEXING_ICB_UPDATE_INTERVAL',
      '500',
      'FIND_INDEXING_POPULARITY_AVE_PERIOD',
      '3',
    ],
  })

  const client = connect({ port })

  await client.updateSchema({
    languages: ['en'],
    types: {
      book: {
        prefix: 'bo',
        fields: {
          name: { type: 'string' },
        },
      },
      something: {
        prefix: 'so',
        fields: {
          name: { type: 'string' },
        },
      },
    },
  })
  await client.destroy()
})

test.after(async (t) => {
  const client = connect({ port }, { loglevel: 'info' })
  await client.delete('root')
  await client.destroy()
  await srv.destroy()
})

test.serial.only(
  'items should not repeat with a different offset',
  async (t) => {
    const client = connect({ port }, { loglevel: 'info' })

    const items: { id: string; name: string }[] = []

    const parentId = await client.set({
      type: 'something',
      name: 'parent',
    })
    for (let index = 0; index < 100; index++) {
      const name = `Book ${index}`
      const id = await client.set({
        type: 'book',
        name,
        parents: [parentId],
      })
      await client.set({
        type: 'something',
        name,
      })
      items.push({ id, name })
    }

    let previousIds: string[] = []
    for (let index = 0; index < 5; index++) {
      const query = {
        books: {
          id: true,
          parents: true,
          $list: {
            // $sort: { $field: 'name', $order: 'asc' },
            $offset: 20 * index,
            $limit: 20,
            $find: {
              $traverse: 'descendants',
              $filter: [
                {
                  $field: 'type',
                  $operator: '=',
                  $value: 'book',
                },
                {
                  $field: 'parents',
                  $operator: 'has',
                  $value: [parentId],
                },
              ],
            },
          },
        },
      }
      const r = await client.get(query)
      const ids = r.books.map(({ id }) => id)
      for (const id of ids) {
        t.false(previousIds.includes(id))
      }
      previousIds = previousIds.concat(ids)
    }

    // console.log(JSON.stringify(previousIds, null, 2))

    await client.destroy()
  }
)
