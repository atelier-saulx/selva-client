import { SelvaClient } from '../..'
import { SetOptions } from '../types'
import { Schema, TypeSchema, FieldSchemaArrayLike } from '../../schema'
import fieldParsers from '.'

export default async (
  client: SelvaClient,
  schema: Schema,
  field: string,
  payload: SetOptions,
  result: string[],
  fields: FieldSchemaArrayLike,
  type: string,
  $lang?: string
): Promise<void> => {
  const arr = payload
  if (!Array.isArray(arr)) {
    if (payload.$delete === true) {
      result.push('7', field, '')
    } else if (payload.$push) {
      result.push('D', field, '')

      const fieldWithIdx = `${field}[-1]`
      const itemsFields = fields.items
      const parser = fieldParsers[itemsFields.type]
      parser(
        client,
        schema,
        fieldWithIdx,
        payload,
        result,
        itemsFields,
        type,
        $lang
      )
    } else if (payload.$unshift) {
      result.push('E', field, '')

      const fieldWithIdx = `${field}[0]`
      const itemsFields = fields.items
      const parser = fieldParsers[itemsFields.type]
      parser(
        client,
        schema,
        fieldWithIdx,
        payload,
        result,
        itemsFields,
        type,
        $lang
      )
    } else if (payload.$assign) {
      const idx = payload.$assign.$idx
      const value = payload.$assign.$value

      if (!idx || !value) {
        throw new Error(
          `$assign missing $idx or $value property ${JSON.stringify(payload)}`
        )
      }

      const fieldWithIdx = `${field}[${idx}]`
      const itemsFields = fields.items
      const parser = fieldParsers[itemsFields.type]
      parser(
        client,
        schema,
        fieldWithIdx,
        payload,
        result,
        itemsFields,
        type,
        $lang
      )
    } else if (payload.$remove) {
      result.push('F', field, `${payload.$removeIdx}`)
    }
  } else {
    const itemsFields = fields.items
    const parser = fieldParsers[itemsFields.type]
    await Promise.all(
      arr.map((payload, index) => {
        const fieldWithIdx = `${field}[${index}]`
        parser(
          client,
          schema,
          fieldWithIdx,
          payload,
          result,
          itemsFields,
          type,
          $lang
        )
      })
    )
  }
}
