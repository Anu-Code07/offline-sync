import 'dart:async';

import 'package:flutter/material.dart';
import 'package:orbitsync/orbitsync.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final controller = SyncEngineController(
    SyncEngine(storage: InMemoryStorageAdapter()),
  );
  await controller.initialize();
  runApp(ExpenseApp(controller: controller));
}

class ExpenseApp extends StatelessWidget {
  const ExpenseApp({required this.controller, super.key});

  final SyncEngineController controller;

  @override
  Widget build(BuildContext context) {
    return SyncScope(
      controller: controller,
      child: MaterialApp(
        title: 'OrbitSync Expenses',
        theme: ThemeData.dark(useMaterial3: true),
        home: const ExpensePage(),
      ),
    );
  }
}

class ExpensePage extends StatelessWidget {
  const ExpensePage({super.key});

  @override
  Widget build(BuildContext context) {
    final expenses = SyncScope.of(context).engine.collection('expenses');
    return Scaffold(
      appBar: AppBar(title: const Text('Offline Expenses')),
      body: SyncCollectionList(
        collection: expenses,
        emptyBuilder: (_) => const Center(child: Text('No expenses')),
        itemBuilder: (context, record) => ListTile(
          leading: const Icon(Icons.receipt_long),
          title: Text(record.data['merchant']?.toString() ?? 'Merchant'),
          subtitle: Text(record.data['category']?.toString() ?? 'General'),
          trailing: Text('\$${record.data['amount'] ?? '0.00'}'),
        ),
      ),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: () {
          unawaited(
            expenses.insert({
              'merchant': 'Coffee Bar',
              'category': 'Meals',
              'amount': 5.75,
              'createdAt': DateTime.now().toUtc().toIso8601String(),
            }),
          );
        },
        icon: const Icon(Icons.add_card),
        label: const Text('Add expense'),
      ),
    );
  }
}
