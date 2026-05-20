import 'dart:async';

import 'package:flutter/material.dart';
import 'package:orbitsync/orbitsync.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final controller = SyncEngineController(
    SyncEngine(storage: InMemoryStorageAdapter()),
  );
  await controller.initialize();
  runApp(TodoApp(controller: controller));
}

class TodoApp extends StatelessWidget {
  const TodoApp({required this.controller, super.key});

  final SyncEngineController controller;

  @override
  Widget build(BuildContext context) {
    return SyncScope(
      controller: controller,
      child: MaterialApp(
        title: 'OrbitSync Todo',
        theme: ThemeData.dark(useMaterial3: true),
        home: const TodoPage(),
      ),
    );
  }
}

class TodoPage extends StatelessWidget {
  const TodoPage({super.key});

  @override
  Widget build(BuildContext context) {
    final todos = SyncScope.of(context).engine.collection('todos');
    return Scaffold(
      appBar: AppBar(
        title: const Text('Offline Todos'),
        actions: <Widget>[
          SyncStateBuilder(
            builder: (context, state) => Padding(
              padding: const EdgeInsets.all(12),
              child: Center(child: Text(state.status.name)),
            ),
          ),
        ],
      ),
      body: SyncCollectionList(
        collection: todos,
        emptyBuilder: (_) => const Center(child: Text('No todos yet')),
        itemBuilder: (context, record) => ListTile(
          title: Text(record.data['title']?.toString() ?? 'Untitled'),
          subtitle: record.isPending ? const Text('Pending sync') : null,
          trailing: Checkbox(
            value: record.data['completed'] as bool? ?? false,
            onChanged: (value) {
              unawaited(todos.update(record.id, {'completed': value ?? false}));
            },
          ),
        ),
      ),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: () {
          unawaited(todos.insert({'title': 'Buy milk', 'completed': false}));
        },
        label: const Text('Add'),
        icon: const Icon(Icons.add),
      ),
    );
  }
}
