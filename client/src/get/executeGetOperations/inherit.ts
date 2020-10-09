import { SelvaClient } from '../../'
import { GetOperationInherit, GetResult } from '../types'

export default async function(
  client: SelvaClient,
  op: GetOperationInherit,
  lang: string,
  db: string
): Promise<GetResult> {
  // TODO: lang for text fields (this goes in create op)
  const prefixes: string = op.types.reduce((acc, t) => {
    const p = client.schemas[db].types[t].prefix
    if (p) {
      acc += p
    }

    return acc
  }, '')

  // TODO: cast based on schema
  if (op.single) {
    const res = await client.redis.selva_inherit(
      {
        name: db
      },
      '___selva_hierarchy',
      op.id,
      prefixes,
      <string>op.sourceField // TODO
    )

    return res.length ? res[0][1] : null
  }

  const fields = Object.keys(op.props).map(f =>
    op.field ? op.field + '.' + f : f
  )
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
    const [f, v] = res[i]
    o[f.slice(op.field.length + 1)] = v
  }

  return o
}
