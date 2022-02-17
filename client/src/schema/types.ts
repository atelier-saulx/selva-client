export type Id = string

export type TypesDb = { idSize: number } & { [key: string]: string }

export type GetSchemaResult = {
  schema: Schema
}

export const FIELD_TYPES = [
  'float',
  'boolean',
  'number',
  'int',
  'string',
  'text',
  'id',
  'digest',
  'url',
  'email',
  'phone',
  'geo',
  'type',
  'timestamp',
  'reference',
  'references',
  'object',
  'record',
  'array',
  'set',
  'json',
  'text',
]

export type FieldType =
  | 'float'
  | 'boolean'
  | 'number'
  | 'int'
  | 'string'
  | 'text'
  | 'id'
  | 'digest'
  | 'url'
  | 'email'
  | 'phone'
  | 'geo'
  | 'type'
  | 'timestamp'

export type TimeSeriesFields = Record<string, FieldSchema>

export type Timeseries = Record<string, TimeSeriesFields> // by type to record of fields that are time series

export type FieldSchemaObject = {
  type: 'object'
  properties: {
    [key: string]: FieldSchema
  }
  meta?: any
  timeseries?: boolean
}

export type FieldSchemaJson = {
  type: 'json'
  properties?: {
    [key: string]: FieldSchema
  }
  meta?: any
  timeseries?: boolean
}

export type FieldSchemaRecord = {
  type: 'record'
  values: FieldSchema
  meta?: any
  timeseries?: boolean
}

export type FieldSchemaReferences = {
  type: 'reference' | 'references'
  bidirectional?: {
    fromField: string
  }
  meta?: any
  timeseries?: boolean
}

export type FieldSchemaOther = {
  type: FieldType
  meta?: any
  timeseries?: boolean
}

export type FieldSchemaArrayLike = {
  search?: { index?: string; type: 'TAG'[] }
  type: 'set' | 'array'
  items: FieldSchema
  meta?: any
  timeseries?: boolean
}

export type FieldSchema =
  | FieldSchemaObject
  | FieldSchemaRecord
  | FieldSchemaJson
  | FieldSchemaArrayLike
  | FieldSchemaReferences
  | FieldSchemaOther

export type Fields = Record<string, FieldSchema>

export type HierarchySchema =
  | false // has to be false but does not work...
  | {
      [key: string]:
        | false // has to be false but does not work...
        | { excludeAncestryWith: string[] }
        | { includeAncestryWith: string[] }
    }

export type TypeSchema = {
  prefix?: string
  hierarchy?: HierarchySchema
  fields?: Fields
}

export type Types = { [key: string]: TypeSchema }

export type Schema = {
  sha?: string
  languages?: string[]
  types: Types
  rootType: Pick<TypeSchema, 'fields' | 'prefix'>
  idSeedCounter?: number
  prefixToTypeMapping?: Record<string, string>
}

export type SchemaOptions = {
  sha?: string
  languages?: string[]
  types?: Types
  rootType: Pick<TypeSchema, 'fields' | 'prefix'>
  idSeedCounter?: number
  prefixToTypeMapping?: Record<string, string>
}
