# sync_storage

Storage adapters for OrbitSync.

Included:

- `InMemoryStorageAdapter`
- `SqliteStorageAdapter`
- `HiveStorageAdapter`
- `IsarStorageAdapter`
- custom `KeyValueStorageBackend`

Adapters preserve mutation ordering, checkpoints, pending state, retry metadata,
and dead-letter queues.
