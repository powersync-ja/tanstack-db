import { createRowDeserializer } from './deserialization'
import { asPowerSyncRecord } from './helpers'
import { convertTableToSchema } from './schema'
import { createSubsetHandlers } from './standalone-subsets'
import type { SyncAppliedReceipt, SyncConfig } from '@tanstack/db'
import type { Table } from '@powersync/common'
import type { TableChanges } from '@powersync/standalone-client'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  EnhancedStandalonePowerSyncCollectionConfig,
  InferPowerSyncOutputType,
  StandalonePowerSyncCollectionConfig,
} from './definitions'
import type { ExtractedTable } from './helpers'

const toError = (cause: unknown) =>
  cause instanceof Error ? cause : new Error(String(cause))

/**
 * Creates the collection config for a collection backed by a standalone PowerSync client.
 *
 * The client keeps synced rows in memory and notifies listeners per table. This sync applies them to the
 * collection, requests the configured Sync Streams, and reports connection and stream errors in the
 * collection's health.
 */
export function createStandaloneCollectionConfig<
  TTable extends Table,
  TSchema extends StandardSchemaV1<any> = never,
>(
  config: StandalonePowerSyncCollectionConfig<TTable, TSchema>,
): EnhancedStandalonePowerSyncCollectionConfig<
  InferPowerSyncOutputType<TTable, TSchema>,
  TSchema
> {
  const {
    client,
    table,
    schema: inputSchema,
    syncMode: _syncMode,
    streams,
    getSubscriptions,
    onError,
    ...restConfig
  } = config
  const deserializationSchema =
    `deserializationSchema` in config ? config.deserializationSchema : null
  const onDeserializationError =
    `onDeserializationError` in config
      ? config.onDeserializationError
      : undefined

  type OutputType = InferPowerSyncOutputType<TTable, TSchema>

  if (table.localOnly) {
    throw new Error(`Local-only table ${table.name} is never synced`)
  }

  // We can do basic runtime validations for columns if not explicit schema has been provided
  const schema = inputSchema ?? (convertTableToSchema(table) as TSchema)
  const deserializeSyncRow = createRowDeserializer<
    ExtractedTable<TTable>,
    OutputType
  >(table.name, deserializationSchema || schema, onDeserializationError)

  const sync: SyncConfig<OutputType, string> = {
    rowUpdateMode: `full`,
    sync: ({
      begin,
      write,
      commit,
      markReady,
      fail,
      reportError,
      reportHealthy,
      truncate,
    }) => {
      const controller = new AbortController()
      let first = true
      let active = true
      let ready = false
      let lastApplied: SyncAppliedReceipt = true
      // Errors reported before the current connection, and on it. A connection that completes a
      // checkpoint has re-resolved every stream, so the earlier errors no longer apply.
      let earlierErrors: Array<Error> = []
      let connectionErrors: Array<Error> = []
      let unlisten = () => {}

      /** This sync run cannot continue: fail it and release the client's resources. */
      const stop = (error: Error) => {
        if (!active) return
        fail(error)
        cleanup()
        onError?.(error)
      }
      /** The client retries connection and stream errors. Before the first ready, they fail the load. */
      const onRetryableError = (error: Error) => {
        if (!active) return
        if (!ready) return stop(error)
        reportError(error)
        connectionErrors.push(error)
        onError?.(error)
      }
      const onStatus = () => {
        if (!active || !ready) return
        const { state } = client.status
        if (state === `syncing`) {
          earlierErrors.push(...connectionErrors)
          connectionErrors = []
        } else if (state === `ready`) {
          for (const error of earlierErrors) reportHealthy(error)
          earlierErrors = []
        }
      }
      const onChanges = (changes: TableChanges) => {
        if (!active) return
        // A row failing validation or a rejected commit would lose rows, so the sync run stops.
        try {
          const writes = [...changes].map(([id, row]) => {
            if (row == null) return { type: `delete` as const, key: id }
            const value = deserializeSyncRow({
              ...row,
              id,
            } as ExtractedTable<TTable>)
            if (asPowerSyncRecord(value).id !== id) {
              throw new Error(`The schema must preserve the PowerSync row id`)
            }
            return { type: `update` as const, value }
          })
          if (!first && !writes.length) return
          begin()
          // The initial snapshot replaces earlier rows, readiness is tracked separately.
          if (first) truncate({ markReady: false })
          for (const operation of writes) write(operation)
          const applied = commit(controller.signal)
          lastApplied = applied
          first = false
          // Never block ingestion on application: TanStack can hold this receipt
          // behind a mutation handler which is itself waiting for confirmWrites().
          if (applied !== true) {
            applied.then(undefined, (cause) => stop(toError(cause)))
          }
        } catch (cause) {
          stop(toError(cause))
        }
      }
      const setReady = () => {
        ready = true
        markReady()
      }

      const unsubscribeStatus = client.subscribeStatus(onStatus)
      const lease = getSubscriptions
        ? undefined
        : client.acquireStreams(streams ?? [], { onError: onRetryableError })
      const subsets = getSubscriptions
        ? createSubsetHandlers(
            client,
            getSubscriptions,
            () => lastApplied,
            onRetryableError,
          )
        : undefined
      const cleanup = () => {
        if (!active) return
        active = false
        controller.abort()
        subsets?.cleanup()
        lease?.release()
        unsubscribeStatus()
        unlisten()
      }

      if (lease) {
        lease.waitForSync(controller.signal).then(
          async () => {
            await lastApplied
            if (active) setReady()
          },
          // Stream errors are reported through the lease, cancellation needs no handling.
          () => {},
        )
      } else {
        // Readiness for on-demand data is tracked by each loadSubset promise.
        // Starting the collection itself requires no remote table snapshot.
        setReady()
      }

      // listen() delivers the current rows synchronously, which can already stop this run.
      unlisten = client.listen(table.name, {
        onChanges,
        onError: onRetryableError,
      })
      if (controller.signal.aborted) unlisten()
      return subsets
        ? {
            loadSubset: subsets.loadSubset,
            unloadSubset: subsets.unloadSubset,
            cleanup,
          }
        : cleanup
    },
  }

  // Keep the deserialization options out of the core config.
  const {
    deserializationSchema: _deserializationSchema,
    onDeserializationError: _onDeserializationError,
    ...collectionConfig
  } = { deserializationSchema, onDeserializationError, ...restConfig }

  return {
    ...collectionConfig,
    schema,
    syncMode: getSubscriptions ? `on-demand` : `eager`,
    getKey: (record: OutputType) => asPowerSyncRecord(record).id,
    utils: { confirmWrites: client.confirmWrites },
    sync,
  }
}
