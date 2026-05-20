# OrbitSync

OrbitSync is a Flutter/Dart offline sync engine SDK for building local-first apps
with automatic synchronization, realtime updates, retry queues, conflict
resolution, delta sync, optimistic writes, and pluggable storage engines.

The repository is a Melos monorepo with publishable packages:

- `sync_core` - storage contracts, sync engine, retry queues, conflict policies,
  delta sync, optimistic mutation APIs, security hooks, and background sync
  abstractions.
- `sync_storage` - in-memory adapter plus SQLite, Hive, and Isar bridge adapters.
- `sync_realtime` - websocket-style realtime transport with reconnect,
  heartbeats, subscriptions, presence, and event streams.
- `sync_crdt` - CRDT utility primitives such as vector clocks, LWW registers, and
  observed-remove sets.
- `sync_flutter` - Flutter controllers/widgets and state-management integration
  helpers.
- `sync_devtools` - Flutter inspector panel for queues, retries, websocket state,
  conflicts, and sync timelines.
- `server_mock` - Express/WebSocket mock backend with latency, packet-drop,
  conflict, reconnect, and failure injection.

```dart
final sync = SyncEngine(
  storage: InMemoryStorageAdapter(),
  transport: HttpSyncTransport(endpoint: Uri.parse('https://api.example.com/sync')),
  realtime: WebSocketRealtimeClient(
    endpoint: Uri.parse('wss://api.example.com/realtime'),
  ),
);

await sync.initialize();

await sync.collection('todos').insert({
  'title': 'Buy milk',
  'completed': false,
});

sync.collection('todos').watch().listen((records) {
  // Instant local updates, then automatic reconciliation with the server.
});
```

See `docs/architecture.md` for the system design and `server_mock/README.md` to
run a local backend.
