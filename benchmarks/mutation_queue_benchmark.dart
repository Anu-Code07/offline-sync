import 'package:orbitsync/orbitsync.dart';

Future<void> main() async {
  final storage = InMemoryStorageAdapter();
  final engine = SyncEngine(storage: storage, batchSize: 500);
  await engine.initialize();

  final collection = engine.collection('benchmark_records');
  final stopwatch = Stopwatch()..start();

  for (var index = 0; index < 100000; index += 1) {
    await collection.insert({
      'index': index,
      'value': 'record-$index',
    });
  }

  stopwatch.stop();
  final pending = await storage.pendingMutations(limit: 100000);
  print('Inserted ${pending.length} optimistic records in ${stopwatch.elapsed}.');
  await engine.close();
}
