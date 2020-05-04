import { SelvaClient } from '..'
import { GetResult, GetOptions } from './types'
import validate from './validate'

async function get(client: SelvaClient, props: GetOptions): Promise<GetResult> {
  await validate(client, props)
  const getResult = await client.fetch(props)
  // TODO: verify props
  return getResult
}

export { get, GetResult, GetOptions }
