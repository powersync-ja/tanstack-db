import type {
  CleanupFn,
  LoadSubsetOptions,
  SyncAppliedReceipt,
  SyncConfigRes,
} from '@tanstack/db'
import type {
  PowerSyncClient,
  StreamError,
  StreamLease,
} from '@powersync/standalone-client'
import type { GetSubscriptions } from './definitions'

/** Utilities to turn a function extracting Sync Stream subscriptions from queries into a {@link SyncConfigRes}. */
export function createSubsetHandlers(
  client: PowerSyncClient,
  getSubscriptions: GetSubscriptions,
  applied: () => SyncAppliedReceipt,
  onError: (error: StreamError) => void,
): SyncConfigRes & { cleanup: CleanupFn } {
  const acquisitions = new Map<
    LoadSubsetOptions,
    {
      lease: StreamLease
      abort: AbortController
      removeListener: () => void
    }
  >()
  let active = true
  const unloadSubset = (options: LoadSubsetOptions) => {
    const acquisition = acquisitions.get(options)
    if (!acquisition) return
    acquisitions.delete(options)
    acquisition.removeListener()
    acquisition.abort.abort()
    acquisition.lease.release()
  }
  const loadSubset = (options: LoadSubsetOptions): Promise<void> => {
    if (!active) throw new Error('PowerSync collection is not syncing')
    options.signal?.throwIfAborted()
    // Mapping failures occur before taking ownership of resources.
    const streams = getSubscriptions(options)
    const lease = client.acquireStreams(streams, { onError })
    const abort = new AbortController()
    const release = () => unloadSubset(options)
    acquisitions.set(options, {
      lease,
      abort,
      removeListener: () =>
        options.signal?.removeEventListener('abort', release),
    })
    options.signal?.addEventListener('abort', release, { once: true })
    const queued = lease.waitForSync(abort.signal)
    return (async () => {
      try {
        await queued
        // Unlike confirmWrites(), loadSubset promises visible data. Never await
        // this receipt in the stream delivery callback itself.
        await applied()
        abort.signal.throwIfAborted()
      } catch (error) {
        // A shared collection commit cannot be canceled by just one observer.
        // If it was already queued, let it become visible before settling the
        // canceled baseline, as required by TanStack's replay contract.
        unloadSubset(options)
        await applied()
        throw error
      }
    })()
  }
  return {
    loadSubset,
    unloadSubset,
    cleanup: () => {
      active = false
      for (const options of acquisitions.keys()) unloadSubset(options)
    },
  }
}
