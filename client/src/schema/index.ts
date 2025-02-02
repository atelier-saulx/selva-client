import { Types, TypeSchema, FieldSchema, InputTypes } from './types'

export * from './types'

export type SchemaMutationType = 'delete_type' | 'change_field' | 'remove_field'

export type SchemaMutations = (
  | {
      mutation: 'delete_type'
      type: string
    }
  | {
      mutation: 'change_field'
      type: string
      path: string[]
      old: FieldSchema
      new: FieldSchema
    }
  | {
      mutation: 'remove_field'
      type: string
      path: string[]
      old: FieldSchema
    }
)[]

export type SchemaOptions = {
  sha?: string
  languages?: string[]
  types?: Types
  rootType?: Pick<TypeSchema, 'fields'>
  idSeedCounter?: number
  prefixToTypeMapping?: Record<string, string>
}

export type SchemaOpts = {
  sha?: string
  languages?: string[]
  types?: InputTypes
  rootType?: Pick<TypeSchema, 'fields' | 'meta'>
  idSeedCounter?: number
  prefixToTypeMapping?: Record<string, string>
}

export const defaultFields: Record<string, FieldSchema> = {
  id: {
    type: 'id',
    // never indexes these - uses in keys
  },
  type: {
    type: 'type',
  },
  children: {
    type: 'references',
  },
  parents: {
    type: 'references',
  },
  ancestors: {
    type: 'references',
  },
  descendants: {
    type: 'references',
  },
  aliases: {
    type: 'set',
    items: { type: 'string' },
  },
  createdAt: {
    type: 'timestamp',
  },
  updatedAt: {
    type: 'timestamp',
  },
}

export const rootDefaultFields: Record<string, FieldSchema> = {
  id: {
    type: 'id',
  },
  type: {
    type: 'type',
  },
  children: {
    type: 'references',
  },
  descendants: {
    type: 'references',
  },
  createdAt: {
    type: 'timestamp',
  },
  updatedAt: {
    type: 'timestamp',
  },
}
