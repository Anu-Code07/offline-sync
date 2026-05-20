# OrbitSync

OrbitSync is a single Flutter/Dart offline sync package for building local-first
apps with automatic synchronization, realtime updates, retry queues, conflict
resolution, delta sync, optimistic writes, and pluggable storage engines.

The public API is available through one package import:

```dart
import 'package:orbitsync/orbitsync.dart';
```

## Installation

Add OrbitSync to your Flutter app:

```yaml
dependencies:
  orbitsync: ^0.1.0
```

For local development in this repository, examples use a path dependency:

```yaml
dependencies:
  orbitsync:
    path: ../..
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

## Quick usage

```dart
final sync = SyncEngine(
  storage: InMemoryStorageAdapter(),
  transport: HttpSyncTransport(
    endpoint: Uri.parse('https://api.example.com'),
    send: (request) async {
      // Adapt this to package:http, dio, or your existing API client.
      return <String, Object?>{
        'records': <Object?>[],
        'acknowledgements': <Object?>[],
      };
    },
  ),
);

await sync.initialize();

final todos = sync.collection('todos');
await todos.insert({
  'title': 'Buy milk',
  'completed': false,
});

todos.watch(options: const QueryOptions(limit: 50)).listen((records) {
  // Instant local updates, then automatic reconciliation with the server.
});
```

## Examples

Example apps live in `examples/`:

- `todo_app` - optimistic local writes and pending sync indicators.
- `chat_app` - offline message queueing.
- `collaborative_notes_app` - conflict-friendly local-first editing.
- `expense_tracker` - retry-friendly expense writes.

Each example imports the single package:

```dart
import 'package:orbitsync/orbitsync.dart';
```

See `docs/usage.md` for a step-by-step guide, `docs/architecture.md` for the
system design, and `server_mock/README.md` to run a local backend.
