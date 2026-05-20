import 'package:sync_core/sync_core.dart';
import 'package:test/test.dart';

void main() {
  test('DeltaPatch tracks changed and removed fields', () {
    final before = SyncRecord(
      collection: 'todos',
      id: '1',
      data: const {'title': 'Milk', 'done': false},
      version: 1,
      updatedAt: DateTime.utc(2026),
    );
    final after = before.copyWith(
      data: const {'title': 'Milk', 'priority': 'high'},
      version: 2,
    );

    final patch = DeltaPatch.between(before: before, after: after);

    expect(patch.changedFields, containsAll(<String>['done', 'priority']));
    expect(patch.values['done'], isNull);
    expect(patch.values['priority'], 'high');
  });

  test('server wins conflict resolver returns remote data', () async {
    final local = SyncRecord(
      collection: 'todos',
      id: '1',
      data: const {'title': 'Local'},
      version: 1,
      updatedAt: DateTime.utc(2026),
    );
    final remote = local.copyWith(
      data: const {'title': 'Remote'},
      version: 2,
      updatedAt: DateTime.utc(2026, 1, 2),
    );

    final resolved = await const ConflictResolver(
      strategy: ConflictStrategy.serverWins,
    ).resolve(ConflictContext(local: local, remote: remote, mutation: null));

    expect(resolved.data['title'], 'Remote');
  });

  test('retry policy stops after max attempts', () {
    final mutation = Mutation(
      id: 'm1',
      collection: 'todos',
      recordId: '1',
      type: MutationType.insert,
      payload: const {},
      sequence: 1,
      clientTimestamp: DateTime.utc(2026),
      idempotencyKey: 'todos/1/1',
      attemptCount: 3,
    );

    const policy = RetryPolicy(maxAttempts: 3, jitterFactor: 0);

    expect(policy.shouldRetry(mutation), isFalse);
  });
}
