import { SetOptions } from '../types'
import { TypeSchema, FieldSchemaJson, FieldSchemaObject } from '../../schema'
import fieldParsers from '.'

export default (
  schemas: Record<string, TypeSchema>,
  field: string,
  payload: SetOptions,
  result: SetOptions,
  fields: FieldSchemaJson,
  type: string
): void => {
  if (payload.$field) {
    // TODO: verify that it references a json field
    result[field] = `___selva_$ref:${payload[field]}`
    return
  }

  if (fields.properties) {
    const obj = {}
    fieldParsers.object(
      schemas,
      field,
      payload,
      obj,
      {
        type: 'object',
        properties: fields.properties
      },
      type
    )
    result[field] = JSON.stringify(obj)
  } else {
    result[field] = JSON.stringify(payload)
  }
  // needs nested json without casting to for exampe, json again
}
