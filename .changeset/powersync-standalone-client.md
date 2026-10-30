---
'@tanstack/powersync-db-collection': minor
---

Accept a standalone `PowerSyncClient` as `client` in `powerSyncCollectionOptions`, syncing rows into memory without a local SQLite database. Client collections use the same `table` and schema options, take their own `onInsert`, `onUpdate` and `onDelete` handlers, request Sync Streams through `streams` or `getSubscriptions`, and expose `collection.utils.confirmWrites()`.
