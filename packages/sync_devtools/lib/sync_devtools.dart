library sync_devtools;

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:sync_core/sync_core.dart';

class SyncDevToolsPanel extends StatefulWidget {
  const SyncDevToolsPanel({
    required this.engine,
    super.key,
  });

  final SyncEngine engine;

  @override
  State<SyncDevToolsPanel> createState() => _SyncDevToolsPanelState();
}

class _SyncDevToolsPanelState extends State<SyncDevToolsPanel> {
  final List<SyncEvent> _events = <SyncEvent>[];
  StreamSubscription<SyncEvent>? _eventSubscription;

  @override
  void initState() {
    super.initState();
    _eventSubscription = widget.engine.events.listen((event) {
      setState(() {
        _events.insert(0, event);
        if (_events.length > 200) {
          _events.removeLast();
        }
      });
    });
  }

  @override
  void dispose() {
    _eventSubscription?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Theme(
      data: theme.copyWith(
        colorScheme: ColorScheme.fromSeed(
          seedColor: const Color(0xFF6C5CE7),
          brightness: Brightness.dark,
        ),
      ),
      child: Scaffold(
        backgroundColor: const Color(0xFF0B1020),
        appBar: AppBar(
          title: const Text('OrbitSync DevTools'),
          actions: <Widget>[
            IconButton(
              tooltip: 'Run sync',
              onPressed: () {
                unawaited(widget.engine.synchronize());
              },
              icon: const Icon(Icons.sync),
            ),
          ],
        ),
        body: StreamBuilder<SyncState>(
          stream: widget.engine.states,
          initialData: widget.engine.state,
          builder: (context, snapshot) {
            final state = snapshot.data ?? const SyncState.initial();
            return FutureBuilder<_QueueSnapshot>(
              future: _loadQueueSnapshot(),
              builder: (context, queueSnapshot) {
                final queues = queueSnapshot.data ?? _QueueSnapshot.empty();
                return ListView(
                  padding: const EdgeInsets.all(16),
                  children: <Widget>[
                    _StatusHeader(state: state),
                    const SizedBox(height: 16),
                    _QueueInspector(snapshot: queues),
                    const SizedBox(height: 16),
                    _Timeline(events: _events),
                  ],
                );
              },
            );
          },
        ),
      ),
    );
  }

  Future<_QueueSnapshot> _loadQueueSnapshot() async {
    final pending = await widget.engine.storage.pendingMutations(limit: 1000);
    final deadLetters = await widget.engine.storage.deadLetters(limit: 1000);
    return _QueueSnapshot(pending: pending, deadLetters: deadLetters);
  }
}

class _StatusHeader extends StatelessWidget {
  const _StatusHeader({required this.state});

  final SyncState state;

  @override
  Widget build(BuildContext context) {
    return _DevCard(
      title: 'Sync State',
      child: Wrap(
        spacing: 12,
        runSpacing: 12,
        children: <Widget>[
          _Metric(label: 'Status', value: state.status.name),
          _Metric(label: 'Pending', value: state.pendingMutations.toString()),
          _Metric(label: 'Dead letters', value: state.deadLetterCount.toString()),
          _Metric(
            label: 'Last sync',
            value: state.lastSyncAt?.toIso8601String() ?? 'never',
          ),
        ],
      ),
    );
  }
}

class _QueueInspector extends StatelessWidget {
  const _QueueInspector({required this.snapshot});

  final _QueueSnapshot snapshot;

  @override
  Widget build(BuildContext context) {
    return _DevCard(
      title: 'Mutation Queues',
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Text('Pending (${snapshot.pending.length})'),
          const SizedBox(height: 8),
          ...snapshot.pending.take(20).map(_MutationTile.new),
          const Divider(),
          Text('Dead letters (${snapshot.deadLetters.length})'),
          const SizedBox(height: 8),
          ...snapshot.deadLetters.take(20).map(_MutationTile.new),
        ],
      ),
    );
  }
}

class _MutationTile extends StatelessWidget {
  const _MutationTile(this.mutation);

  final Mutation mutation;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      dense: true,
      contentPadding: EdgeInsets.zero,
      title: Text('${mutation.type.name} ${mutation.collection}/${mutation.recordId}'),
      subtitle: Text(
        'seq=${mutation.sequence} attempts=${mutation.attemptCount} fields=${mutation.changedFields.join(', ')}',
      ),
      trailing: mutation.lastError == null
          ? null
          : Tooltip(
              message: mutation.lastError!,
              child: const Icon(Icons.error_outline, color: Colors.orangeAccent),
            ),
    );
  }
}

class _Timeline extends StatelessWidget {
  const _Timeline({required this.events});

  final List<SyncEvent> events;

  @override
  Widget build(BuildContext context) {
    return _DevCard(
      title: 'Sync Timeline',
      child: Column(
        children: events
            .map(
              (event) => ListTile(
                dense: true,
                contentPadding: EdgeInsets.zero,
                title: Text(event.type),
                subtitle: Text(event.message),
                trailing: Text(event.timestamp.toIso8601String()),
              ),
            )
            .toList(growable: false),
      ),
    );
  }
}

class _Metric extends StatelessWidget {
  const _Metric({
    required this.label,
    required this.value,
  });

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: 180,
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: const Color.fromRGBO(255, 255, 255, 0.05),
        borderRadius: BorderRadius.circular(12),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Text(label, style: Theme.of(context).textTheme.labelMedium),
          const SizedBox(height: 6),
          Text(value, style: Theme.of(context).textTheme.titleMedium),
        ],
      ),
    );
  }
}

class _DevCard extends StatelessWidget {
  const _DevCard({
    required this.title,
    required this.child,
  });

  final String title;
  final Widget child;

  @override
  Widget build(BuildContext context) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: <Widget>[
            Text(title, style: Theme.of(context).textTheme.titleLarge),
            const SizedBox(height: 12),
            child,
          ],
        ),
      ),
    );
  }
}

class _QueueSnapshot {
  const _QueueSnapshot({
    required this.pending,
    required this.deadLetters,
  });

  factory _QueueSnapshot.empty() {
    return const _QueueSnapshot(
      pending: <Mutation>[],
      deadLetters: <Mutation>[],
    );
  }

  final List<Mutation> pending;
  final List<Mutation> deadLetters;
}
