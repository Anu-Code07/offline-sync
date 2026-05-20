# Usage

OrbitSync is published as one package. Import the umbrella entrypoint in most
apps:

```dart
import 'package:orbitsync/orbitsync.dart';
```

Use focused entrypoints only when you want to keep imports narrow:

```dart
import 'package:orbitsync/sync_core.dart';
import 'package:orbitsync/sync_storage.dart';
```

## 1. Add the dependency

```yaml
dependencies:
  orbitsync: ^0.1.0
```

When running the repository examples locally:

```yaml
dependencies:
  orbitsync:
    path: ../..
```

## 2. Create an engine

Start with the in-memory adapter while prototyping:

```dart
final sync = SyncEngine(storage: InMemoryStorageAdapter());
await sync.initialize();
```

For production, provide a durable adapter and a transport:

```dart
final sync = SyncEngine(
  storage: SqliteStorageAdapter(executor: sqliteExecutor),
  transport: HttpSyncTransport(
    endpoint: Uri.parse('https://api.example.com'),
    send: apiClient.sendSyncRequest,
  ),
  security: SecureSyncConfig(
    tokenProvider: tokenStore.readAccessToken,
  ),
);
await sync.initialize();
```

## 3. Write local-first data

```dart
final todos = sync.collection('todos');

final id = await todos.insert({
  'title': 'Buy milk',
  'completed': false,
});

await todos.update(id, {'completed': true});
```

Writes update local storage immediately. OrbitSync queues mutations, retries
transient failures, and reconciles server acknowledgements in the background.

## 4. Read reactively

```dart
todos.watch(options: const QueryOptions(limit: 50)).listen((records) {
  // Render local state immediately, including pending changes.
});
```

In Flutter, wrap your app with `SyncScope` and use `SyncCollectionList`:

```dart
final controller = SyncEngineController(sync);
await controller.initialize();

runApp(
  SyncScope(
    controller: controller,
    child: const App(),
  ),
);
```

```dart
SyncCollectionList(
  collection: todos,
  emptyBuilder: (_) => const Center(child: Text('No todos yet')),
  itemBuilder: (context, record) {
    return ListTile(
      title: Text(record.data['title']?.toString() ?? 'Untitled'),
      subtitle: record.isPending ? const Text('Pending sync') : null,
    );
  },
);
```

## 5. Handle conflicts

Use a built-in strategy for simple cases:

```dart
final sync = SyncEngine(
  storage: InMemoryStorageAdapter(),
  conflictResolver: const ConflictResolver(
    strategy: ConflictStrategy.mergeFields,
  ),
);
```

Use a manual resolver when your records have business rules:

```dart
final sync = SyncEngine(
  storage: InMemoryStorageAdapter(),
  conflictResolver: ConflictResolver(
    strategy: ConflictStrategy.manual,
    resolver: (context) {
      return {
        ...context.remote.data,
        ...context.local.data,
        'mergedAt': DateTime.now().toUtc().toIso8601String(),
      };
    },
  ),
);
```

## 6. Inspect sync state

Embed the DevTools panel in a debug-only route:

```dart
SyncDevToolsPanel(engine: sync);
```

The panel shows sync status, queued mutations, dead letters, retry errors, and
timeline events.
