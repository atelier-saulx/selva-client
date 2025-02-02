import test from 'ava'
import { connect } from '../src/index'
import { Schema, SchemaOptions, Fields } from '../src/schema'
import { start } from '@saulx/selva-server'
import { wait } from './assertions'
import getPort from 'get-port'

const mangleResults = (
  correctSchema: Schema | SchemaOptions,
  schemaResult: Schema
) => {
  if (!correctSchema.sha) {
    delete schemaResult.sha
  }

  for (const type in schemaResult.types) {
    if (!correctSchema.types[type].prefix) {
      delete schemaResult.types[type].prefix
    }
  }
  delete schemaResult.idSeedCounter
  delete schemaResult.prefixToTypeMapping
}

test.serial('schemas - basic', async (t) => {
  const port = await getPort()
  const server = await start({
    port,
  })
  const client = connect({ port })

  await wait(100)

  const defaultFields: Fields = {
    id: {
      type: 'id',
    },
    type: {
      type: 'type',
    },
    title: {
      type: 'text',
    },
    parents: {
      type: 'references',
    },
    children: {
      type: 'references',
    },
    ancestors: {
      type: 'references',
    },
    descendants: {
      type: 'references',
    },
    aliases: {
      type: 'set',
      items: { type: 'string' },
    },
    name: {
      type: 'string',
    },
    createdAt: { type: 'timestamp' },
    updatedAt: { type: 'timestamp' },
  }

  const schema: SchemaOptions = {
    languages: ['nl', 'en'],
    rootType: {
      fields: {
        children: { type: 'references' },
        descendants: { type: 'references' },
        id: { type: 'id' },
        type: { type: 'type' },
        value: { type: 'number' },
        createdAt: { type: 'timestamp' },
        updatedAt: { type: 'timestamp' },
      },
    },
    types: {
      league: {
        fields: {
          ...defaultFields,
        },
      },
      person: {
        fields: {
          ...defaultFields,
        },
      },
      video: {
        fields: {
          ...defaultFields,
        },
      },
      vehicle: {
        fields: {
          ...defaultFields,
        },
      },
      family: {
        fields: {
          ...defaultFields,
        },
      },
      match: {
        prefix: 'ma',
        hierarchy: {
          team: { excludeAncestryWith: ['league'] },
          video: false,
          person: { includeAncestryWith: ['family'] },
          $default: { excludeAncestryWith: ['vehicle'] },
        },
        fields: {
          ...defaultFields,
          smurky: {
            meta: {
              yesh: 'a meta value',
              data: ['in an array'],
            },
            type: 'set',
            items: {
              type: 'object', // stored as json in this case (scince you start with a set)
              properties: {
                interval: {
                  type: 'array',
                  items: {
                    type: 'timestamp',
                  },
                },
                url: { type: 'url' },
              },
            },
          },
          flurpy: {
            type: 'object',
            properties: {
              snurkels: {
                type: 'string',
              },
            },
          },
          flapperdrol: {
            type: 'json',
          },
          video: {
            type: 'object',
            properties: {
              mp4: {
                type: 'url',
              },
              hls: {
                type: 'url',
              },
              pano: {
                type: 'url',
              },
              overlays: {
                type: 'array',
                items: {
                  type: 'json', // needs to be json!
                  properties: {
                    interval: {
                      type: 'array',
                      items: {
                        type: 'timestamp',
                      },
                    },
                    url: { type: 'url' },
                  },
                },
              },
            },
          },
        },
      },
    },
  }

  await client.updateSchema(schema)

  const { schema: schemaResult } = await client.getSchema()

  // make sure meta is accessible
  t.deepEqual(schemaResult.types.match.fields.smurky.meta, {
    yesh: 'a meta value',
    data: ['in an array'],
  })

  // @ts-ignore
  schema.rootType.prefix = 'ro'

  mangleResults(schema, schemaResult)

  t.deepEqual(schemaResult, schema, 'correct schema')

  t.deepEqualIgnoreOrder(
    Object.keys(schema.types),
    ['league', 'person', 'video', 'vehicle', 'family', 'match'],
    'correct type map'
  )

  // t.true(
  //   (await client.redis.keys('*')).includes('idx:default'),
  //   'made redis-search index for default'
  // )

  // t.true(
  //   (await client.redis.keys('*')).includes('idx:hls'),
  //   'made redis-search index for hls'
  // )

  await client.updateSchema({
    types: {
      match: {
        fields: {
          ...defaultFields,
          smurky: {
            type: 'set',
            items: {
              type: 'object', // stored as json in this case (scince you start with a set)
              properties: {
                interval: {
                  type: 'array',
                  items: {
                    type: 'timestamp',
                  },
                },
                url: { type: 'url' },
              },
            },
          },
        },
      },
    },
  })

  let newResult = (await client.getSchema()).schema

  mangleResults(schema, newResult)
  t.deepEqual(
    newResult,
    schema,
    'correct schema after setting the same without meta'
  )

  await client.updateSchema({
    types: {
      match: {
        fields: {
          smurky: {
            meta: 'overriden with string',
            type: 'set',
            items: {
              type: 'object', // stored as json in this case (scince you start with a set)
              properties: {
                interval: {
                  type: 'array',
                  items: {
                    type: 'timestamp',
                  },
                },
                url: { type: 'url' },
              },
            },
          },
        },
      },
    },
  })

  newResult = (await client.getSchema()).schema
  t.deepEqual(
    newResult.types.match.fields.smurky.meta,
    'overriden with string',
    'can overwrite meta'
  )

  // test that you can't set custom types with 'ro' as prefix
  let e = await t.throwsAsync(
    client.updateSchema({
      types: {
        flurpydurpy: {
          prefix: 'ro',
          fields: {
            niceStrField: { type: 'string' },
          },
        },
      },
    })
  )

  t.true(e.stack.includes('Prefix ro is already in use'))

  // test that you can't set custom types with an already used prefix as prefix
  e = await t.throwsAsync(
    client.updateSchema({
      types: {
        flurpydurpy: {
          prefix: 'ma',
          fields: {
            niceStrField: { type: 'string' },
          },
        },
      },
    })
  )

  t.true(e.stack.includes('Prefix ma is already in use'))

  await t.notThrowsAsync(
    client.updateSchema({
      types: {
        flurpydurpy: {
          prefix: 'fl',
          fields: {
            niceStrField: { type: 'string' },
            niceObject: {
              type: 'object',
              properties: {
                niceStrField: { type: 'string' },
              },
            },
          },
        },
      },
    })
  )

  // make sure you can't add nonsensical field types on new type
  e = await t.throwsAsync(
    client.updateSchema({
      types: {
        durpyflurpy: {
          fields: {
            niceStrField: { type: <'string'>'strin' },
          },
        },
      },
    })
  )

  t.true(e.stack.includes(`Field niceStrField has an unsupported field type`))

  // make sure you can't add nonsensical field types on existing type
  e = await t.throwsAsync(
    client.updateSchema({
      types: {
        flurpydurpy: {
          fields: {
            notSoNiceStrField: { type: <'string'>'strin' },
          },
        },
      },
    })
  )

  t.true(
    e.stack.includes(`Field notSoNiceStrField has an unsupported field type`)
  )

  // make sure you can't add nonsensical field types on existing type and existing object nested field
  e = await t.throwsAsync(
    client.updateSchema({
      types: {
        flurpydurpy: {
          fields: {
            niceObject: {
              type: 'object',
              properties: {
                helloBad: { type: <'string'>'strin' },
              },
            },
          },
        },
      },
    })
  )

  t.true(
    e.stack.includes(`Field niceObject.helloBad has an unsupported field type`)
  )

  await client.updateSchema({
    types: {
      match: {
        fields: {
          flurpy: {
            type: 'object',
            properties: {
              snurpie: {
                type: 'string',
              },
            },
          },
        },
      },
    },
  })

  t.deepEqual(
    (await client.getSchema()).schema.types.match.fields.flurpy,
    {
      type: 'object',
      properties: {
        snurkels: {
          type: 'string',
        },
        snurpie: {
          type: 'string',
        },
      },
    },
    'added field to object schema'
  )

  // sends hierarchy update
  await client.updateSchema({
    types: {
      match: {
        hierarchy: {
          team: { excludeAncestryWith: ['league'] },
          video: false,
          $default: { excludeAncestryWith: ['vehicle'] },
        },
      },
    },
  })

  t.deepEqual(
    (await client.getSchema()).schema.types.match.hierarchy,
    {
      team: { excludeAncestryWith: ['league'] },
      video: false,
      $default: { excludeAncestryWith: ['vehicle'] },
    },
    'updated hierarchy schema'
  )

  await client.updateSchema({
    types: {
      match: {
        fields: {
          flurpbird: { type: 'digest' },
          date: { type: 'timestamp' },
        },
      },
    },
  })

  await client.set({
    type: 'match',
    video: {
      mp4: 'https://flappie.com/clowns.mp4',
    },
    flurpbird: 'hello',
    date: 100000,
    title: {
      en: 'best match',
    },
    children: [
      {
        type: 'person',
        parents: { $add: 'root' },
      },
    ],
    flapperdrol: { smurky: true },
  })

  await client.set({
    $id: 'root',
    value: 9001,
  })

  // add some tests for it
  await client.destroy()
  await server.destroy()
  await t.connectionsAreEmpty()
})
