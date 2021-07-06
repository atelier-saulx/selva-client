import { Aggregate, GetOperationAggregate, GetOptions, Sort } from '../types'
import { createAst, optimizeTypeFilters } from '@saulx/selva-query-ast-parser'

const createAggregateOperation = (
  aggregate: Aggregate,
  props: GetOptions,
  id: string,
  field: string,
  limit: number = -1,
  offset: number = 0,
  sort?: Sort | Sort[]
): GetOperationAggregate => {
  const op: GetOperationAggregate = {
    type: 'aggregate',
    id,
    props,
    field: field.substr(1),
    sourceField: field.substr(1),
    recursive: !!aggregate.$recursive,
    options: {
      limit,
      offset,
      sort: Array.isArray(sort) ? sort[0] : sort || undefined,
    },
    function:
      typeof aggregate.$function === 'string'
        ? { name: aggregate.$function }
        : { name: aggregate.$function.$name, args: aggregate.$function.$args },
  }

  if (aggregate.$traverse) {
    if (typeof aggregate.$traverse === 'string') {
      op.sourceField = aggregate.$traverse
    } else if (Array.isArray(aggregate.$traverse)) {
      op.inKeys = aggregate.$traverse
    }
  }

  if (aggregate.$filter) {
    const ast = createAst(aggregate.$filter)

    if (ast) {
      optimizeTypeFilters(ast)
      op.filter = ast
    }
  }

  console.log('CREATEAD OP', op)
  return op
}

export default createAggregateOperation
