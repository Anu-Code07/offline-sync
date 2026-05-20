library sync_flutter;

import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:sync_core/sync_core.dart';

class SyncEngineController extends ChangeNotifier {
  SyncEngineController(this.engine);

  final SyncEngine engine;
  StreamSubscription<SyncState>? _subscription;
  SyncState _state = const SyncState.initial();

  SyncState get state => _state;

  Future<void> initialize() async {
    await engine.initialize();
    _state = engine.state;
    _subscription = engine.states.listen((state) {
      _state = state;
      notifyListeners();
    });
    notifyListeners();
  }

  Future<void> synchronize() => engine.synchronize();

  Future<void> pause() => engine.pause();

  Future<void> resume() => engine.resume();

  @override
  void dispose() {
    _subscription?.cancel();
    unawaited(engine.close());
    super.dispose();
  }
}

class SyncScope extends InheritedNotifier<SyncEngineController> {
  const SyncScope({
    required SyncEngineController controller,
    required super.child,
    super.key,
  }) : super(notifier: controller);

  static SyncEngineController of(BuildContext context) {
    final scope = context.dependOnInheritedWidgetOfExactType<SyncScope>();
    assert(scope != null, 'No SyncScope found in the widget tree.');
    return scope!.notifier!;
  }
}

typedef SyncStateWidgetBuilder = Widget Function(BuildContext context, SyncState state);

class SyncStateBuilder extends StatelessWidget {
  const SyncStateBuilder({
    required this.builder,
    super.key,
  });

  final SyncStateWidgetBuilder builder;

  @override
  Widget build(BuildContext context) {
    final controller = SyncScope.of(context);
    return AnimatedBuilder(
      animation: controller,
      builder: (context, _) => builder(context, controller.state),
    );
  }
}

class SyncCollectionList extends StatelessWidget {
  const SyncCollectionList({
    required this.collection,
    required this.itemBuilder,
    this.emptyBuilder,
    this.loadingBuilder,
    this.options = const QueryOptions(),
    super.key,
  });

  final SyncCollection collection;
  final QueryOptions options;
  final Widget Function(BuildContext context, SyncRecord record) itemBuilder;
  final WidgetBuilder? emptyBuilder;
  final WidgetBuilder? loadingBuilder;

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<List<SyncRecord>>(
      stream: collection.watch(options: options),
      builder: (context, snapshot) {
        if (!snapshot.hasData) {
          return loadingBuilder?.call(context) ?? const SizedBox.shrink();
        }
        final records = snapshot.data!;
        if (records.isEmpty) {
          return emptyBuilder?.call(context) ?? const SizedBox.shrink();
        }
        return ListView.builder(
          itemCount: records.length,
          itemBuilder: (context, index) => itemBuilder(context, records[index]),
        );
      },
    );
  }
}

class SyncBlocBridge {
  const SyncBlocBridge(this.engine);

  final SyncEngine engine;

  Stream<SyncState> get stateStream => engine.states;

  Future<void> loadSyncState() => engine.synchronize();
}

class SyncRiverpodBridge {
  const SyncRiverpodBridge(this.engine);

  final SyncEngine engine;

  Stream<SyncState> stateProvider() => engine.states;

  SyncCollection collectionProvider(String name) => engine.collection(name);
}

class SyncGetxBridge {
  SyncGetxBridge(this.engine);

  final SyncEngine engine;
  final ValueNotifier<SyncState> state = ValueNotifier<SyncState>(const SyncState.initial());
  StreamSubscription<SyncState>? _subscription;

  void bind() {
    _subscription ??= engine.states.listen((nextState) {
      state.value = nextState;
    });
  }

  Future<void> dispose() async {
    await _subscription?.cancel();
    state.dispose();
  }
}

class SyncProviderModel extends ChangeNotifier {
  SyncProviderModel(this.engine) {
    _subscription = engine.states.listen((state) {
      current = state;
      notifyListeners();
    });
  }

  final SyncEngine engine;
  late final StreamSubscription<SyncState> _subscription;
  SyncState current = const SyncState.initial();

  @override
  void dispose() {
    _subscription.cancel();
    super.dispose();
  }
}

class FlutterBackgroundSyncScheduler implements BackgroundSyncScheduler {
  const FlutterBackgroundSyncScheduler({
    required this.registerTask,
    required this.cancelTask,
  });

  final Future<void> Function(String taskId, Duration frequency) registerTask;
  final Future<void> Function(String taskId) cancelTask;

  @override
  Future<void> registerPeriodicSync({
    required String taskId,
    required Duration frequency,
  }) {
    return registerTask(taskId, frequency);
  }

  @override
  Future<void> cancel(String taskId) => cancelTask(taskId);
}
