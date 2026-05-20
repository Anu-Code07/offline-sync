import 'package:sync_core/sync_core.dart';
import 'package:sync_storage/sync_storage.dart';
import 'package:test/test.dart';

void main() {
  test('in-memory storage persists records and mutation ordering', () async {
    final storage = InMemoryStorageAdapter();
    await storage.initialize();

    await storage.upsertRecord(
      SyncRecord(
        collection: 'todos',
        id: '1',
        data: const {'title': 'Milk'},
        version: 1,
        updatedAt: DateTime.utc(2026),
      ),
    );

    final sequence = await storage.nextSequence();
    await storage.enqueueMutation(
      Mutation(
        id: 'm1',
        collection: 'todos',
        recordId: '1',
        type: MutationType.insert,
        payload: const {'title': 'Milk'},
        sequence: sequence,
        clientTimestamp: DateTime.utc(2026),
        idempotencyKey: 'todos/1/1',
      ),
    );

    expect((await storage.query('todos')).single.id, '1');
    expect((await storage.pendingMutations()).single.sequence, sequence);
  });
}
