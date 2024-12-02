import fs from 'fs'
import test from 'ava'
import { connect } from '../src/index'
import { startWithTimeseries } from '@saulx/selva-server'
import { wait } from './assertions'
import getPort from 'get-port'
import { createPatch, applyPatch } from '@saulx/diff'
import { deepCopy } from '@saulx/utils'

let srv
let port: number

test.before(async (t) => {
  port = await getPort()
  srv = await startWithTimeseries({
    port,
  })
  await new Promise((resolve, _reject) => {
    setTimeout(resolve, 100)
  })
})

test.beforeEach(async (t) => {
  const client = connect({ port })
  await client.updateSchema({
    languages: ['en', 'de', 'nl'],
    rootType: {
      fields: {
        value: { type: 'number' },
        nested: {
          type: 'object',
          properties: {
            fun: { type: 'string' },
          },
        },
      },
    },
    types: {
      lekkerType: {
        prefix: 'vi',
        fields: {
          strRec: {
            type: 'record',
            values: {
              type: 'string',
            },
          },
          textRec: {
            type: 'record',
            values: {
              type: 'text',
            },
          },
          objRec: {
            type: 'record',
            values: {
              type: 'object',
              properties: {
                floatArray: { type: 'array', items: { type: 'float' } },
                intArray: { type: 'array', items: { type: 'int' } },
                objArray: {
                  type: 'array',
                  items: {
                    type: 'object',
                    properties: {
                      hello: { type: 'string' },
                      value: { type: 'int' },
                    },
                  },
                },
                hello: {
                  type: 'string',
                },
                nestedRec: {
                  type: 'record',
                  values: {
                    type: 'object',
                    properties: {
                      value: {
                        type: 'number',
                      },
                      hello: {
                        type: 'string',
                      },
                    },
                  },
                },
                value: {
                  type: 'number',
                },
                stringValue: {
                  type: 'string',
                },
              },
            },
          },
          thing: { type: 'set', items: { type: 'string' } },
          ding: {
            type: 'object',
            properties: {
              dong: { type: 'set', items: { type: 'string' } },
              texty: { type: 'text' },
              dung: { type: 'number' },
              dang: {
                type: 'object',
                properties: {
                  dung: { type: 'number' },
                  dunk: { type: 'string' },
                },
              },
              dunk: {
                type: 'object',
                properties: {
                  ding: { type: 'number' },
                  dong: { type: 'number' },
                },
              },
            },
          },
          dong: { type: 'json' },
          dingdongs: { type: 'array', items: { type: 'string' } },
          floatArray: { type: 'array', items: { type: 'float' } },
          intArray: { type: 'array', items: { type: 'int' } },
          refs: { type: 'references' },
          value: { type: 'number', timeseries: true },
          age: { type: 'number' },
          auth: {
            type: 'json',
            timeseries: true,
          },
          title: { type: 'text' },
          description: { type: 'text' },
          image: {
            timeseries: true,
            type: 'object',
            properties: {
              thumb: { type: 'string' },
              poster: { type: 'string' },
              pixels: { type: 'int' },
            },
          },
        },
      },
      custom: {
        prefix: 'cu',
        fields: {
          value: { type: 'number' },
          age: { type: 'number' },
          auth: {
            type: 'json',
          },
          title: { type: 'text' },
          description: { type: 'text' },
          image: {
            type: 'object',
            properties: {
              thumb: { type: 'string' },
              poster: { type: 'string' },
              pixels: { type: 'int' },
            },
          },
        },
      },
      club: {
        prefix: 'cl',
        fields: {
          value: { type: 'number' },
          age: { type: 'number' },
          auth: {
            type: 'json',
          },
          title: { type: 'text' },
          description: { type: 'text' },
          image: {
            type: 'object',
            properties: {
              thumb: { type: 'string' },
              poster: { type: 'string' },
              pixels: { type: 'int' },
            },
          },
        },
      },
      match: {
        prefix: 'ma',
        fields: {
          title: { type: 'text' },
          value: { type: 'number' },
          description: { type: 'text' },
        },
      },
      yesno: {
        prefix: 'yn',
        fields: {
          bolYes: { type: 'boolean' },
          bolNo: { type: 'boolean' },
        },
      },
      want: {
        prefix: 'wa',
        fields: {
          intTimeseries: { type: 'int', timeseries: true },
        },
      },
    },
  })

  // A small delay is needed after setting the schema
  await new Promise((r) => setTimeout(r, 100))

  await client.destroy()
})

test.after.always(async (t) => {
  const client = connect({ port })
  await client.delete('root')
  await client.destroy()
  await srv.destroy()
  await t.connectionsAreEmpty()
  await wait(500)
})

// TODO: this will work once branch schema update valiation is merged: https://github.com/atelier-saulx/selva/blob/schema-update-validation/client/src/schema/types.ts#L10
// import { FIELD_TYPES } from '../src/schema/types'
// import { SELVA_TO_SQL_TYPE } from "../../server/src/server/timeseriesWorker"
//
// test.serial('ensure type mappings are in sync', async (t) => {
//   const selvaTypes = new Set(FIELD_TYPES)
//   const timeseriesTypes = new Set(Object.keys(SELVA_TO_SQL_TYPE))
//
//   for (let type of selvaTypes) {
//     if (!timeseriesTypes.has(type)) {
//       t.fail(`${type} is missing from the timeseries mapping, this will make us fail to manage timeseries for this type`)
//     }
//   }
//   t.true(selvaTypes.size  <= timeseriesTypes.size)
// })
//
// TODO: in filters
//jsonb_extract_path_text(from_json jsonb, VARIADIC path_elems text[])

test[
  !(
    fs.existsSync('/usr/lib/postgresql/12/bin/postgres') ||
    fs.existsSync('/usr/local/Cellar/postgresql@12')
  )
    ? 'skip'
    : 'serial'
]('get - basic value types timeseries', async (t) => {
  const client = connect({ port })

  // TODO: removet his manual step
  await client.pg.connect()

  await client.set({
    $id: 'viA',
    title: {
      en: 'nice!',
    },
    value: 25,
    auth: {
      // role needs to be different , different roles per scope should be possible
      role: {
        id: ['root'],
        type: 'admin',
      },
    },
    image: {
      thumb: 'lol',
      pixels: 2042,
    },
  })

  const servers = await client.redis.smembers({ type: 'registry' }, 'servers')
  const serverData = await Promise.all(
    servers.map((server) => {
      return client.redis.hgetall({ type: 'registry' }, server)
    })
  )
  console.log('BLABLA', serverData)

  await wait(4e3)

  await client.set({
    $id: 'viA',
    image: {
      thumb: 'lol 2',
    },
  })

  console.log(
    'YESYES',
    JSON.stringify(
      await client.get({
        things: {
          $list: {
            $find: {
              $traverse: 'descendants',
            },
          },
          id: true,
          title: true,
          value: true,
          thumb: {
            $field: 'image.thumb',
          },
          image: true,
          allValues: {
            $field: 'value',
            $list: { $limit: 10 },
          },
          filteredValues: {
            $list: {
              $find: {
                $traverse: 'value',
                $filter: [
                  {
                    $field: 'value',
                    $operator: '>',
                    $value: 0,
                    $or: {
                      $field: 'value',
                      $operator: '=',
                      $value: 0,
                    },
                  },
                  {
                    $field: 'value',
                    $operator: '<',
                    $value: 1000,
                  },
                  {
                    $field: 'ts',
                    $operator: '>',
                    $value: 'now-5d',
                  },
                ],
              },
              $sort: {
                $field: 'value',
                // $order: 'asc',
                $order: 'desc',
              },
            },
          },
          thumbnails: {
            thumb: true,
            $list: {
              $find: {
                $traverse: 'image',
                $filter: [
                  {
                    $field: 'thumb',
                    $operator: '=',
                    $value: ['lol', 'lol 2'],
                  },
                  {
                    $field: 'pixels',
                    $operator: '>',
                    $value: '1024',
                  },
                ],
              },
            },
          },
          thumbnails2: {
            $all: true,
            $list: {
              $find: {
                $traverse: 'image',
              },
            },
          },
        },
      }),
      null,
      2
    )
  )

  let i = 3
  let hmmhmm = await client.get({
    $id: 'viA',
    values: {
      $field: 'value',
      $list: { $limit: 5 },
    },
    thumbnails2: {
      $all: true,
      $list: {
        $find: {
          $traverse: 'image',
        },
        $limit: 5,
      },
    },
  })

  await wait(2e3)

  const obs = client.observe({
    $id: 'viA',
    id: true,
    title: true,
    value: true,
    values: {
      $field: 'value',
      $list: { $limit: 5 },
    },
  })

  obs.subscribe((yes) => {
    console.log('yes', yes)
  })

  await wait(1e3)

  const subs = await client.redis.selva_subscriptions_list('___selva_hierarchy')
  console.log(
    'SUBBY SUB',
    await client.redis.selva_subscriptions_debug('___selva_hierarchy', 'viA')
  )
  for (const sid of subs) {
    console.log(
      'SUB',
      sid,
      await client.redis.selva_subscriptions_debug('___selva_hierarchy', sid)
    )
  }

  setInterval(async () => {
    await client.set({
      $id: 'viA',
      value: 25 + i,
      image: {
        thumb: `lol ${i}`,
      },
    })

    // const wutwut = await client.get({
    //   $firstEval: false,
    //   $id: 'viA',
    //   values: {
    //     $field: 'value',
    //     $list: { $limit: 5 },
    //   },
    //   valuesTs: { $raw: 'value._ts' },
    //   thumbnails2: {
    //     $all: true,
    //     $list: {
    //       $find: {
    //         $traverse: 'image',
    //       },
    //       $limit: 5,
    //     },
    //   },
    //   imageTs: { $raw: 'image._ts' },
    // })

    // console.log('WUT WUT', wutwut)

    // const a = { values: deepCopy(hmmhmm.values) }
    // const b = { values: wutwut.values }
    // const patch = createPatch(a, b, {
    //   parseDiffFunctions: true,
    // })
    // // @ts-ignore
    // console.log('PATCH', a.values.length, a, b, JSON.stringify(patch, null, 2))
    // const applied = applyPatch(a, patch)
    // console.log('APPLIED PATCH', applied)

    // hmmhmm = await client.get({
    //   $id: 'viA',
    //   values: {
    //     $field: 'value',
    //     $list: { $limit: 5 },
    //   },
    //   thumbnails2: {
    //     $all: true,
    //     $list: {
    //       $find: {
    //         $traverse: 'image',
    //       },
    //       $limit: 5,
    //     },
    //   },
    // })
    // console.log('HMMHMM', JSON.stringify(hmmhmm, null, 2))

    i++
  }, 2e3)

  await wait(5000e3)

  await client.delete('root')

  await client.destroy()
})

test[
  !(
    fs.existsSync('/usr/lib/postgresql/12/bin/postgres') ||
    fs.existsSync('/usr/local/Cellar/postgresql@12')
  )
    ? 'skip'
    : 'serial'
]('$increment on int', async (t) => {
  // test.only('$increment on int', async (t) => {
  const client = connect({ port })
  const id = await client.set({
    type: 'want',
    intTimeseries: 0,
  })

  await client.set({
    $id: id,
    intTimeseries: { $increment: 1 },
  })

  t.is(
    (
      await client.get({
        $id: id,
        intTimeseries: true,
      })
    ).intTimeseries,
    1
  )

  await client.delete('root')

  await client.destroy()
})
