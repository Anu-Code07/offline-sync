# OrbitSync

OrbitSync is a single Flutter/Dart offline sync package for building local-first
apps with automatic synchronization, realtime updates, retry queues, conflict
resolution, delta sync, optimistic writes, and pluggable storage engines.

The public API is available through one package import:

```dart
import 'package:orbitsync/orbitsync.dart';
```

The package also keeps focused library entrypoints for teams that prefer narrow
imports:

- `orbitsync.dart` - umbrella export for the complete package.
- `sync_core.dart` - storage contracts, sync engine, retry queues, conflict
  policies, delta sync, optimistic mutation APIs, security hooks, and background
  sync abstractions.
- `sync_storage.dart` - in-memory adapter plus SQLite, Hive, and Isar bridge
  adapters.
- `sync_realtime.dart` - websocket-style realtime transport with reconnect,
  heartbeats, subscriptions, presence, and event streams.
- `sync_crdt.dart` - CRDT utility primitives such as vector clocks, LWW
  registers, and observed-remove sets.
- `sync_flutter.dart` - Flutter controllers/widgets and state-management
  integration helpers.
- `sync_devtools.dart` - Flutter inspector panel for queues, retries, websocket
  state, conflicts, and sync timelines.

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
