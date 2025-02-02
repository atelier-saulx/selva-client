import { Id } from '../schema/index'
// import { FilterAST, Rpn, Fork } from '@saulx/selva-query-ast-parser'

export declare type Value = (string | number) | (string | number)[]
export declare type FilterAST = {
  $field: string
  $operator:
    | '='
    | '>'
    | '<'
    | '..'
    | '!='
    | 'has'
    | 'includes'
    | 'distance'
    | 'exists'
    | 'notExists'
    | 'textSearch'
  $value?: Value
  hasNow?: true
}
export declare type Fork = {
  $and?: (Fork | FilterAST)[]
  $or?: (Fork | FilterAST)[]
  ids?: string[]
  isFork: true
}
export type Inherit =
  | boolean
  | {
      $type?: string | string[]
      $item?: Id | Id[]
      $merge?: boolean
      $deepMerge?: boolean
      $required?: Id | Id[]
    }

export type GeoFilter = {
  $operator: 'distance'
  $field: string
  $value: {
    $lat: number
    $lon: number
    $radius: number
  }
  $and?: Filter
  $or?: Filter
}

export type ExistsFilter = {
  $operator: 'exists' | 'notExists'
  $field: string
  $value?: undefined // makes compiling this easier, nice...
  $and?: Filter
  $or?: Filter
}

export type Filter =
  | ExistsFilter
  | GeoFilter
  | {
      $operator: '=' | '!=' | '>' | '<' | '..' | 'has' | 'includes' | 'textSearch'
      $field: string
      $value: string | number | (string | number)[]
      $and?: Filter
      $or?: Filter
    }

export type TraverseOptions = {
  $db?: string
  $id?: string
  $field: string
  // TODO: add $filter, $limit, $offset
}

export type TraverseByTypeExpression =
  | false
  | string
  | {
      $first?: TraverseByTypeExpression[]
      $all?: TraverseByTypeExpression[]
    }

export type TraverseByType = {
  $any: TraverseByTypeExpression
  [k: string]: TraverseByTypeExpression
}

export type Find = {
  $db?: string
  $traverse?:
    | 'descendants'
    | 'ancestors'
    | string
    | string[]
    | TraverseOptions
    | TraverseByType
  $recursive?: boolean
  $filter?: Filter | Filter[]
  $find?: Find
}

export type Aggregate = {
  $db?: string
  $traverse?:
    | 'descendants'
    | 'ancestors'
    | string
    | string[]
    | TraverseOptions
    | TraverseByType
  $filter?: Filter | Filter[]
  $recursive?: boolean
  $function?: string | { $name: string; $args: string[] }
  $find?: Find
  $sort?: Sort
  $offset?: number
  $limit?: number
}

export type Sort = {
  $field: string
  $order?: 'asc' | 'desc'
}

export type List =
  | true
  | {
      $offset?: number
      $limit?: number
      $sort?: Sort | Sort[]
      $find?: Find
      $aggregate?: Aggregate
      $inherit?: Inherit
    }

export type GetField<T> = {
  $field?: string | string[]
  $inherit?: Inherit
  $list?: List
  $find?: Find
  $aggregate?: Aggregate
  $default?: T
  $all?: boolean
  $value?: any
}

// want with $ come on :D
export type Item = {
  [key: string]: any
}

// update $language for default + text (algebraic)
export type GetItem<T = Item> = {
  [P in keyof T]?: T[P] extends Item[]
    ? GetItem<T>[] | true
    : T[P] extends object
    ? (GetItem<T[P]> & GetField<T>) | true
    : T[P] extends number
    ? T[P] | GetField<T[P]> | true
    : T[P] extends string
    ? T[P] | GetField<T[P]> | true
    : T[P] extends boolean
    ? T[P] | GetField<T[P]>
    : (T[P] & GetField<T[P]>) | true
} &
  GetField<T> & {
    [key: string]: any
  }

export type GetResult = {
  [key: string]: any
}

export type GetOptions = GetItem & {
  $trigger?: { $event: 'created' | 'updated' | 'deleted'; $filter?: Filter }
  $id?: Id | Id[]
  $alias?: string | string[]
  $version?: string
  $language?: string
  $rawAncestors?: true
}

export type ObserveEventOptions = GetItem & {
  $filter?: Filter
}

type GetOperationCommon = {
  id: string
  field: string
  sourceField: string | string[]
}

export type GetOperationAggregate = GetOperationCommon & {
  type: 'aggregate'
  props: GetOptions
  filter?: Fork
  inKeys?: string[]
  function: { name: string; args?: string[] }
  recursive?: boolean
  byType?: TraverseByType
  options: { limit: number; offset: number; sort?: Sort | undefined }
  nested?: GetOperationFind
  isTimeseries?: boolean
}

export type GetOperationFind = GetOperationCommon & {
  type: 'find'
  props: GetOptions
  single?: boolean
  filter?: Fork
  inKeys?: string[]
  nested?: GetOperationFind
  isNested?: boolean
  recursive?: boolean
  byType?: TraverseByType
  options: { limit: number; offset: number; sort?: Sort | undefined }
  isTimeseries?: boolean
}

export type GetOperationInherit = GetOperationCommon & {
  type: 'inherit'
  props: GetOptions
  types: string[]
  single?: boolean
  item?: boolean
  required?: string[]
  merge?: boolean
  deepMerge?: boolean
}

export type GetOperation =
  | (GetOperationCommon & {
      type: 'db'
      default?: any
    })
  | (GetOperationCommon & {
      type: 'raw'
      default?: any
    })
  | { type: 'value'; value: string; field: string }
  | (WithOptional<GetOperationCommon, 'id' | 'sourceField'> & {
      type: 'nested_query'
      props: GetOptions
      fromReference?: boolean
    })
  | { type: 'array_query'; props: GetOptions[]; field: string; id: string }
  | GetOperationFind
  | GetOperationAggregate
  | GetOperationInherit
