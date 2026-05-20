# API Guide

All APIs are available from the single publishable package:

```dart
import 'package:orbitsync/orbitsync.dart';
```

If you want narrower imports, use the focused entrypoints from the same package,
for example:

```dart
import 'package:orbitsync/sync_core.dart';
import 'package:orbitsync/sync_storage.dart';
```

## Initialize

```dart
final sync = SyncEngine(
  storage: InMemoryStorageAdapter(),
  transport: HttpSyncTransport(
    endpoint: Uri.parse('https://api.example.com'),
    send: myHttpClient.sendSyncRequest,
  ),
);

await sync.initialize();
```

For local-only prototypes and tests, omit the transport and use the in-memory
storage adapter:

```dart
final sync = SyncEngine(storage: InMemoryStorageAdapter());
await sync.initialize();
```

## Collections

```dart
final todos = sync.collection('todos');

final id = await todos.insert({
  'title': 'Buy milk',
  'completed': false,
});

await todos.update(id, {'completed': true});
await todos.delete(id);
```

Each write is committed locally first, marked pending, and queued for sync. If
the device is offline, the queue flushes after connectivity returns.

## Reactive reads

```dart
todos.watch(
  options: QueryOptions(limit: 50),
).listen((records) {
  // Records are emitted immediately from local storage and updated after sync.
});
```

Use `SyncCollectionList` in Flutter UI when you want a widget that listens to a
collection stream:

```dart
SyncCollectionList(
  collection: todos,
  emptyBuilder: (_) => const Text('No todos yet'),
  itemBuilder: (context, record) {
    return ListTile(
      title: Text(record.data['title']?.toString() ?? 'Untitled'),
      subtitle: record.isPending ? const Text('Pending sync') : null,
    );
  },
);
```

## Conflict resolution

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

Use `ConflictStrategy.mergeFields` for simple non-overlapping field merges, and
use `ConflictStrategy.manual` when domain invariants must be preserved.

## Security hooks

```dart
final secure = SecureSyncConfig(
  tokenProvider: () async => tokenStore.readAccessToken(),
  requestSigner: (context) => signer.sign(context.body),
);
```

Token refresh, secure storage, SQLCipher, and E2E encryption are intentionally
hook-based so apps can integrate their existing auth stack without coupling core
sync behavior to a single vendor.

## DevTools panel

Embed the inspector behind a debug-only route:

```dart
SyncDevToolsPanel(engine: sync);
```

The panel reads sync state, pending mutations, dead letters, and timeline events
from the same engine instance your app uses.
