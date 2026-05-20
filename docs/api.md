# API Guide

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

## Reactive reads

```dart
todos.watch(
  options: QueryOptions(limit: 50),
).listen((records) {
  // Records are emitted immediately from local storage and updated after sync.
});
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
