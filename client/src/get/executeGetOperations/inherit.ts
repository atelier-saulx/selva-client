import { SelvaClient } from '../../'
import {
  GetOperationInherit,
  GetResult,
  GetOptions,
  GetOperation,
  Fork
} from '../types'
import { getNestedSchema, setNestedResult } from '../utils'
import executeGetOperations, { TYPE_CASTS } from './'
import { FieldSchema } from '../../schema'
import { ast2rpn } from '@saulx/selva-query-ast-parser'

async function inheritItem(
  client: SelvaClient,
  op: GetOperationInherit,
  lang: string,
  db: string
): Promise<GetResult> {
  const schema = client.schemas[db]

  const remapped: Record<string, string> = {}
  const fields = Object.keys(op.props).map(f => {
    f = f.slice(op.field.length + 1)
    if (typeof op.props[f] === 'string') {
      remapped[<string>op.props[f]] = f
      return <string>op.props[f]
    }

    return f
  })

  let fork: Fork = {
    isFork: true,
    $or: op.types.map(t => {
      return {
        $operator: '=',
        $field: 'type',
        $value: t
      }
    })
  }

  if (op.required) {
    fork = {
      isFork: true,
      $and: [
        {
          isFork: true,
          $and: op.required.map(f => {
            return {
              $operator: 'exists',
              $field: f
            }
          })
        },
        fork
      ]
    }
  }

  const rpn = ast2rpn(fork)

  const [id] = await client.redis.selva_hierarchy_find(
    {
      name: db
    },
    '___selva_hierarchy',
    'bfs',
    'ancestors',
    'limit',
    1,
    op.id,
    ...rpn
  )

  if (!id) {
    return null
  }

  const ops: GetOperation[] = fields.map(f => {
    return {
      id,
      type: 'db',
      field: f,
      sourceField: f
    }
  })

  const o = await executeGetOperations(client, lang, db, ops)
  return o
}

async function getObject(
  client: SelvaClient,
  db: string,
  field: string,
  _fieldSchema: FieldSchema,
  lang: string | undefined,
  id: string
): Promise<GetResult> {
  const op: GetOperation = {
    type: 'db',
    id,
    field: field,
    sourceField: field
  }

  const o = await executeGetOperations(client, lang, db, [op])
  return o[field]
}

export default async function(
  client: SelvaClient,
  op: GetOperationInherit,
  lang: string,
  db: string
): Promise<GetResult> {
  const schema = client.schemas[db]
  if (op.item) {
    return inheritItem(client, op, lang, db)
  }

  const prefixes: string = op.types.reduce((acc, t) => {
    if (t === 'root') {
      return 'ro'
    }

    const p = client.schemas[db].types[t].prefix
    if (p) {
      acc += p
    }

    return acc
  }, '')

  if (op.single) {
    const fs = getNestedSchema(
      schema,
      schema.types[op.types[0]].prefix,
      <string>op.sourceField
    )

    const res = await client.redis.selva_inherit(
      {
        name: db
      },
      '___selva_hierarchy',
      op.id,
      prefixes,
      <string>op.sourceField // TODO?
    )

    const v = res.length ? res[0][2] : null

    if (v === '___selva_$object') {
      return await getObject(
        client,
        db,
        <string>op.sourceField,
        fs,
        lang,
        res[0][0]
      )
    }

    if (TYPE_CASTS[fs.type]) {
      return TYPE_CASTS[fs.type](v)
    }

    return v
  }

  const remapped: Record<string, string> = {}
  const fields = Object.keys(op.props).map(f => {
    if (typeof op.props[f] === 'string') {
      remapped[<string>op.props[f]] = f
      return <string>op.props[f]
    }

    return f
  })

  const res = await client.redis.selva_inherit(
    {
      name: db
    },
    '___selva_hierarchy',
    op.id,
    prefixes,
    ...fields
  )

  const o: GetResult = {}
  for (let i = 0; i < res.length; i++) {
    let [idx, f, v] = res[i]
    const fs = getNestedSchema(schema, schema.types[op.types[0]].prefix, f)

    if (v === '___selva_$object') {
      return await getObject(client, db, <string>op.sourceField, fs, lang, idx)
    }

    const typeCast = TYPE_CASTS[fs.type]

    if (remapped[f]) {
      f = remapped[f]
    }

    o[f.slice(op.field.length + 1)] = typeCast ? typeCast(v) : v
  }

  return o
}
