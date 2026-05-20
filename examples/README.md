# OrbitSync Examples

Each example uses the single root package:

```dart
import 'package:orbitsync/orbitsync.dart';
```

Local example pubspecs depend on the repository root:

```yaml
dependencies:
  orbitsync:
    path: ../..
```

## Apps

- `todo_app` - demonstrates optimistic local writes, pending indicators, and
  collection watches.
- `chat_app` - demonstrates offline message queueing with a simple input flow.
- `collaborative_notes_app` - demonstrates conflict-friendly updates using a
  merge-fields resolver.
- `expense_tracker` - demonstrates durable expense writes that can retry after
  failures.

## Running locally

From any example directory:

```sh
flutter pub get
flutter run
```

The examples use `InMemoryStorageAdapter` so they run without external services.
To test server sync, provide an `HttpSyncTransport` and point it at the mock
server described in `../server_mock/README.md`.
