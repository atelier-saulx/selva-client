import { SelvaClient } from '../..'
import { GetOperation, GetOptions } from '../types'
import { getNestedSchema } from '../utils'
import find from './find'

const list = (
  client: SelvaClient,
  db: string,
  props: GetOptions,
  id: string,
  field: string
): GetOperation => {
  const fieldSchema = getNestedSchema(
    client.schemas[db],
    id,
    <string>props.$field || field.substr(1)
  )

  const isTimeseries = fieldSchema && fieldSchema.timeseries

  if (props.$list === true) {
    return {
      type: 'find',
      id,
      props,
      field: field.substr(1),
      sourceField: <string>props.$field || field.substr(1),
      options: {
        limit: -1, // no limit
        offset: 0,
      },
      isTimeseries,
    }
  } else if (props.$list.$find) {
    // TODO: add isTimeseries
    return find(
      props.$list.$find,
      props,
      id,
      field,
      false,
      props.$list.$limit,
      props.$list.$offset,
      props.$list.$sort
    )
  } else {
    return {
      type: 'find',
      id,
      props,
      field: field.substr(1),
      sourceField: <string>props.$field || field.substr(1),
      options: {
        limit: props.$list.$limit || -1,
        offset: props.$list.$offset || 0,
        sort: Array.isArray(props.$list.$sort)
          ? props.$list.$sort[0]
          : props.$list.$sort || undefined,
      },
      isTimeseries,
    }
  }
}

export default list
