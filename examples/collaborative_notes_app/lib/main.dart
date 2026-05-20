import 'dart:async';

import 'package:flutter/material.dart';
import 'package:sync_core/sync_core.dart';
import 'package:sync_flutter/sync_flutter.dart';
import 'package:sync_storage/sync_storage.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final controller = SyncEngineController(
    SyncEngine(
      storage: InMemoryStorageAdapter(),
      conflictResolver: const ConflictResolver(strategy: ConflictStrategy.mergeFields),
    ),
  );
  await controller.initialize();
  runApp(NotesApp(controller: controller));
}

class NotesApp extends StatelessWidget {
  const NotesApp({required this.controller, super.key});

  final SyncEngineController controller;

  @override
  Widget build(BuildContext context) {
    return SyncScope(
      controller: controller,
      child: MaterialApp(
        title: 'OrbitSync Notes',
        theme: ThemeData.dark(useMaterial3: true),
        home: const NotesPage(),
      ),
    );
  }
}

class NotesPage extends StatelessWidget {
  const NotesPage({super.key});

  @override
  Widget build(BuildContext context) {
    final notes = SyncScope.of(context).engine.collection('notes');
    return Scaffold(
      appBar: AppBar(title: const Text('Collaborative Notes')),
      body: SyncCollectionList(
        collection: notes,
        emptyBuilder: (_) => const Center(child: Text('Create a note')),
        itemBuilder: (context, record) => ListTile(
          title: Text(record.data['title']?.toString() ?? 'Untitled'),
          subtitle: Text(record.data['body']?.toString() ?? ''),
          onTap: () {
            unawaited(
              notes.update(record.id, {
                'body': '${record.data['body'] ?? ''}\nLocal edit',
                'editedAt': DateTime.now().toUtc().toIso8601String(),
              }),
            );
          },
        ),
      ),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: () {
          unawaited(
            notes.insert({
              'title': 'Design notes',
              'body': 'Start typing offline...',
              'editedAt': DateTime.now().toUtc().toIso8601String(),
            }),
          );
        },
        icon: const Icon(Icons.note_add),
        label: const Text('New note'),
      ),
    );
  }
}
