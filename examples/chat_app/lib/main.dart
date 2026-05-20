import 'dart:async';

import 'package:flutter/material.dart';
import 'package:orbitsync/orbitsync.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final controller = SyncEngineController(
    SyncEngine(storage: InMemoryStorageAdapter()),
  );
  await controller.initialize();
  runApp(ChatApp(controller: controller));
}

class ChatApp extends StatelessWidget {
  const ChatApp({required this.controller, super.key});

  final SyncEngineController controller;

  @override
  Widget build(BuildContext context) {
    return SyncScope(
      controller: controller,
      child: MaterialApp(
        title: 'OrbitSync Chat',
        theme: ThemeData.dark(useMaterial3: true),
        home: const ChatPage(),
      ),
    );
  }
}

class ChatPage extends StatefulWidget {
  const ChatPage({super.key});

  @override
  State<ChatPage> createState() => _ChatPageState();
}

class _ChatPageState extends State<ChatPage> {
  final TextEditingController _messageController = TextEditingController();

  @override
  void dispose() {
    _messageController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final messages = SyncScope.of(context).engine.collection('messages');
    return Scaffold(
      appBar: AppBar(title: const Text('Offline Chat')),
      body: Column(
        children: <Widget>[
          Expanded(
            child: SyncCollectionList(
              collection: messages,
              itemBuilder: (context, record) => ListTile(
                title: Text(record.data['body']?.toString() ?? ''),
                subtitle: Text(record.data['author']?.toString() ?? 'anonymous'),
              ),
            ),
          ),
          Padding(
            padding: const EdgeInsets.all(12),
            child: Row(
              children: <Widget>[
                Expanded(
                  child: TextField(
                    controller: _messageController,
                    decoration: const InputDecoration(hintText: 'Message'),
                  ),
                ),
                IconButton(
                  onPressed: () {
                    final body = _messageController.text.trim();
                    if (body.isEmpty) {
                      return;
                    }
                    unawaited(
                      messages.insert({
                        'body': body,
                        'author': 'local-user',
                        'sentAt': DateTime.now().toUtc().toIso8601String(),
                      }),
                    );
                    _messageController.clear();
                  },
                  icon: const Icon(Icons.send),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}
