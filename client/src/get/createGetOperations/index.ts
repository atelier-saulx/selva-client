import { Schema, SelvaClient } from '../../'
import { GetOperation, GetOptions } from '../types'
import find from './find'
import aggregate from './aggregate'
import list from './list'
import all from './all'
import createInheritOperation from './inherit'
import { getNestedSchema } from '../utils'
import { id2type } from '../../util'

export default function createGetOperations(
  client: SelvaClient,
  props: GetOptions,
  id: string,
  field: string,
  db: string,
  ops: GetOperation[] = [],
  passedOnSchema?: Schema
): GetOperation[] {
  const schema = passedOnSchema || client.schemas[db]

  // TODO: handle single case of $fieldsByType here based on type of passed id
  if (props.$value) {
    ops.push({
      type: 'value',
      field: field.slice(1),
      value: props.$value,
    })
  } else if (props.$raw) {
    ops.push({
      type: 'raw',
      id,
      field: field.slice(1),
      sourceField: props.$raw === true ? field.slice(1) : props.$raw,
    })
  } else if (props.$id && field) {
    ops.push({
      type: 'nested_query',
      field: field.slice(1),
      props,
    })
  } else if (Array.isArray(props)) {
    ops.push({
      type: 'array_query',
      id,
      field: field.slice(1),
      props,
    })
  } else if (props.$list) {
    ops.push(list(client, db, props, id, field, passedOnSchema))
  } else if (props.$find) {
    ops.push(
      find(
        client,
        db,
        props.$find,
        props,
        id,
        field,
        true,
        undefined,
        undefined,
        undefined,
        false,
        passedOnSchema
      )
    )
  } else if (props.$aggregate) {
    ops.push(
      aggregate(
        client,
        db,
        props.$aggregate,
        props,
        id,
        field,
        undefined,
        undefined,
        undefined,
        passedOnSchema
      )
    )
  } else if (props.$default) {
    ops.push({
      type: 'db',
      id,
      field: field.slice(1),
      sourceField: props.$field || field.slice(1),
      default: props.$default,
    })
  } else if (props.$inherit) {
    createInheritOperation(
      client,
      props.$inherit,
      props,
      id,
      field.slice(1),
      db,
      ops,
      passedOnSchema
    )
  } else if (props.$all) {
    if (props.$field) {
      const $field = Array.isArray(props.$field) ? props.$field : [props.$field]
      all(client, props, id, $field[0], db, ops)
    } else {
      all(client, props, id, field, db, ops)
    }
  } else if (props.$field) {
    const fs = getNestedSchema(
      schema,
      id,
      Array.isArray(props.$field) ? props.$field[0] : props.$field
    )

    if (fs && fs.type === 'reference') {
      ops.push({
        type: 'nested_query',
        field: field.slice(1),
        props: Object.assign({}, props, { $field: undefined }),
        id,
        sourceField: props.$field,
        fromReference: true,
      })

      return
    }

    ops.push({
      type: 'db',
      id,
      field: field.slice(1),
      sourceField: <string[]>props.$field,
    })
  } else if (typeof props === 'object') {
    const fs = getNestedSchema(schema, id, field.slice(1))

    if (fs && fs.type === 'reference') {
      if (props.$flatten) {
        const parts = field.slice(1).split('.')
        const flattened = parts.slice(0, parts.length - 1).join('.')
        ops.push({
          type: 'nested_query',
          field: flattened,
          props,
          id,
          sourceField: field.slice(1),
          fromReference: true,
        })
      } else {
        ops.push({
          type: 'nested_query',
          field: field.slice(1),
          props,
          id,
          fromReference: true,
        })
      }

      return
    }

    for (const key in props) {
      if (key.startsWith('$')) {
        continue
      }
      createGetOperations(
        client,
        props[key],
        id,
        field + '.' + key,
        db,
        ops,
        passedOnSchema
      )
    }

    if (props.$fieldsByType) {
      const type =
        id === 'root'
          ? 'root'
          : client.schemas[db].prefixToTypeMapping[id2type(id)]

      const additionalFields = props.$fieldsByType[type]
      if (additionalFields) {
        createGetOperations(
          client,
          additionalFields,
          id,
          field,
          db,
          ops,
          passedOnSchema
        )
      }
    }
  } else {
    ops.push({
      type: 'db',
      id,
      field: field.slice(1),
      sourceField: field.slice(1),
    })
  }
  return ops
}
