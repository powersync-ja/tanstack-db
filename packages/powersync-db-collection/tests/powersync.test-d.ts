import { describe, expectTypeOf, it } from 'vitest'
import { Schema, Table, column } from '@powersync/node'
import { createCollection } from '@tanstack/db'
import { z } from 'zod'
import { powerSyncCollectionOptions } from '../src'
import type { PowerSyncDatabase } from '@powersync/node'
import type {
  PowerSyncClient,
  PowerSyncCollectionUtils,
  StandalonePowerSyncCollectionUtils,
} from '../src'

const APP_SCHEMA = new Schema({
  documents: new Table({
    name: column.text,
    author: column.text,
    created_at: column.text,
  }),
})

describe(`PowerSync collection type tests`, () => {
  it(`should type collection.utils as PowerSyncCollectionUtils after createCollection`, () => {
    const options = powerSyncCollectionOptions({
      database: {} as PowerSyncDatabase,
      table: APP_SCHEMA.props.documents,
    })
    const collection = createCollection(options)

    // Verify that collection.utils is typed as PowerSyncCollectionUtils, not UtilsRecord
    const utils: PowerSyncCollectionUtils<
      (typeof APP_SCHEMA.props)['documents']
    > = collection.utils
    expectTypeOf(utils.getMeta).toBeFunction()
    expectTypeOf(collection.utils.getMeta).toBeFunction()

    type InsertParams = Parameters<NonNullable<typeof options.onInsert>>[0]
    type InsertMutation = InsertParams[`transaction`][`mutations`][0]

    expectTypeOf<InsertMutation[`key`]>().toEqualTypeOf<string>()
    expectTypeOf<
      InsertMutation[`collection`][`utils`][`getMeta`]
    >().toBeFunction()

    // @ts-expect-error PowerSync Collection does not expose Electric acknowledgement helpers
    collection.utils.awaitTxId(1)
  })

  it(`types a no-schema comparator against the inferred SQLite row`, () => {
    powerSyncCollectionOptions({
      database: {} as PowerSyncDatabase,
      table: APP_SCHEMA.props.documents,
      compare: (left, right) => {
        expectTypeOf(left.id).toEqualTypeOf<string>()
        expectTypeOf(left.name).toEqualTypeOf<string | null>()
        expectTypeOf(left.author).toEqualTypeOf<string | null>()
        expectTypeOf(left.created_at).toEqualTypeOf<string | null>()
        return (left.name ?? ``).localeCompare(right.name ?? ``)
      },
    })
  })

  it(`types compare against transformed schema output`, () => {
    const schema = z.object({
      id: z.string(),
      name: z.string().nullable(),
      author: z.string().nullable(),
      created_at: z
        .string()
        .nullable()
        .transform((value) => (value ? new Date(value) : null)),
    })

    powerSyncCollectionOptions({
      database: {} as PowerSyncDatabase,
      table: APP_SCHEMA.props.documents,
      schema,
      onDeserializationError: () => {},
      compare: (left, right) => {
        expectTypeOf(left).toEqualTypeOf<z.infer<typeof schema>>()
        return (
          (left.created_at?.getTime() ?? 0) - (right.created_at?.getTime() ?? 0)
        )
      },
    })

    powerSyncCollectionOptions({
      database: {} as PowerSyncDatabase,
      table: APP_SCHEMA.props.documents,
      schema,
      onDeserializationError: () => {},
      compare: (left, right) => {
        // @ts-expect-error transformed dates do not expose string methods
        return left.created_at!.localeCompare(right.created_at!)
      },
    })
  })

  it(`types collection.utils as StandalonePowerSyncCollectionUtils for a client`, () => {
    const options = powerSyncCollectionOptions({
      client: {} as PowerSyncClient,
      table: APP_SCHEMA.props.documents,
      streams: [{ name: `documents` }],
      onInsert: async ({ transaction, collection }) => {
        expectTypeOf(transaction.mutations[0].modified.name).toEqualTypeOf<
          string | null
        >()
        await collection.utils.confirmWrites()
      },
    })
    const collection = createCollection(options)

    const utils: StandalonePowerSyncCollectionUtils = collection.utils
    expectTypeOf(utils.confirmWrites).toBeFunction()
    // @ts-expect-error a standalone client has no local SQLite table to describe
    collection.utils.getMeta()
  })

  it(`types a client collection against transformed schema output`, () => {
    const schema = z.object({
      id: z.string(),
      name: z.string().nullable(),
      author: z.string().nullable(),
      created_at: z
        .string()
        .nullable()
        .transform((value) => (value ? new Date(value) : null)),
    })

    powerSyncCollectionOptions({
      client: {} as PowerSyncClient,
      table: APP_SCHEMA.props.documents,
      schema,
      onDeserializationError: () => {},
      syncMode: `on-demand`,
      getSubscriptions: () => [{ name: `documents` }],
      compare: (left, right) => {
        expectTypeOf(left).toEqualTypeOf<z.infer<typeof schema>>()
        return (
          (left.created_at?.getTime() ?? 0) - (right.created_at?.getTime() ?? 0)
        )
      },
    })
  })

  it(`rejects SQLite-only options for a client`, () => {
    powerSyncCollectionOptions({
      client: {} as PowerSyncClient,
      table: APP_SCHEMA.props.documents,
      // @ts-expect-error batching reads applies to a local SQLite database only
      syncBatchSize: 10,
    })

    // @ts-expect-error getSubscriptions requires syncMode: 'on-demand'
    powerSyncCollectionOptions({
      client: {} as PowerSyncClient,
      table: APP_SCHEMA.props.documents,
      getSubscriptions: () => [],
    })
  })
})
