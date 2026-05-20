library sync_core;

import 'dart:async';
import 'dart:math';

typedef JsonMap = Map<String, Object?>;
typedef RecordPredicate = bool Function(SyncRecord record);
typedef ConflictMerge = FutureOr<JsonMap> Function(ConflictContext context);
typedef RequestSigner = FutureOr<JsonMap> Function(SignedRequestContext context);
typedef TokenProvider = FutureOr<String?> Function();
typedef EncryptionCodec = FutureOr<List<int>> Function(List<int> bytes);

enum MutationType { insert, update, delete }

enum SyncStatus { idle, syncing, paused, offline, failed }

enum ConflictStrategy {
  lastWriteWins,
  serverWins,
  clientWins,
  mergeFields,
  manual,
  timestamp,
  version,
}

enum ConnectivityKind { wifi, mobile, offline, captivePortal, unknown }

enum RealtimeConnectionState {
  disconnected,
  connecting,
  connected,
  reconnecting,
  failed,
}

class SyncException implements Exception {
  const SyncException(this.message, {this.cause});

  final String message;
  final Object? cause;

  @override
  String toString() => 'SyncException($message)';
}

class Clock {
  const Clock();

  DateTime now() => DateTime.now().toUtc();
}

class SyncRecord {
  const SyncRecord({
    required this.collection,
    required this.id,
    required this.data,
    required this.version,
    required this.updatedAt,
    this.isDeleted = false,
    this.isPending = false,
    this.vector = const <String, int>{},
    this.metadata = const <String, Object?>{},
  });

  final String collection;
  final String id;
  final JsonMap data;
  final int version;
  final DateTime updatedAt;
  final bool isDeleted;
  final bool isPending;
  final Map<String, int> vector;
  final JsonMap metadata;

  SyncRecord copyWith({
    JsonMap? data,
    int? version,
    DateTime? updatedAt,
    bool? isDeleted,
    bool? isPending,
    Map<String, int>? vector,
    JsonMap? metadata,
  }) {
    return SyncRecord(
      collection: collection,
      id: id,
      data: data ?? this.data,
      version: version ?? this.version,
      updatedAt: updatedAt ?? this.updatedAt,
      isDeleted: isDeleted ?? this.isDeleted,
      isPending: isPending ?? this.isPending,
      vector: vector ?? this.vector,
      metadata: metadata ?? this.metadata,
    );
  }
}

class Mutation {
  const Mutation({
    required this.id,
    required this.collection,
    required this.recordId,
    required this.type,
    required this.payload,
    required this.sequence,
    required this.clientTimestamp,
    required this.idempotencyKey,
    this.changedFields = const <String>{},
    this.baseVersion,
    this.snapshot,
    this.attemptCount = 0,
    this.nextAttemptAt,
    this.lastError,
    this.metadata = const <String, Object?>{},
  });

  final String id;
  final String collection;
  final String recordId;
  final MutationType type;
  final JsonMap payload;
  final Set<String> changedFields;
  final int sequence;
  final int? baseVersion;
  final DateTime clientTimestamp;
  final String idempotencyKey;
  final SyncRecord? snapshot;
  final int attemptCount;
  final DateTime? nextAttemptAt;
  final String? lastError;
  final JsonMap metadata;

  Mutation copyWith({
    int? attemptCount,
    DateTime? nextAttemptAt,
    String? lastError,
    JsonMap? metadata,
  }) {
    return Mutation(
      id: id,
      collection: collection,
      recordId: recordId,
      type: type,
      payload: payload,
      changedFields: changedFields,
      sequence: sequence,
      baseVersion: baseVersion,
      clientTimestamp: clientTimestamp,
      idempotencyKey: idempotencyKey,
      snapshot: snapshot,
      attemptCount: attemptCount ?? this.attemptCount,
      nextAttemptAt: nextAttemptAt ?? this.nextAttemptAt,
      lastError: lastError,
      metadata: metadata ?? this.metadata,
    );
  }
}

class QueryOptions {
  const QueryOptions({
    this.limit,
    this.offset = 0,
    this.where,
    this.includeDeleted = false,
  });

  final int? limit;
  final int offset;
  final RecordPredicate? where;
  final bool includeDeleted;
}

class SyncCheckpoint {
  const SyncCheckpoint({
    required this.collection,
    required this.cursor,
    required this.updatedAt,
  });

  final String collection;
  final String cursor;
  final DateTime updatedAt;
}

abstract interface class StorageAdapter {
  Future<void> initialize();

  Future<void> close();

  Future<void> upsertRecord(SyncRecord record);

  Future<SyncRecord?> getRecord(String collection, String id);

  Future<void> deleteRecord(String collection, String id, {DateTime? deletedAt});

  Future<List<SyncRecord>> query(String collection, {QueryOptions options});

  Stream<List<SyncRecord>> watchCollection(String collection, {QueryOptions options});

  Future<int> nextSequence();

  Future<void> enqueueMutation(Mutation mutation);

  Future<List<Mutation>> pendingMutations({int limit = 50, DateTime? now});

  Future<void> markMutationSynced(String mutationId);

  Future<void> updateMutation(Mutation mutation);

  Future<void> moveToDeadLetter(Mutation mutation, String reason);

  Future<List<Mutation>> deadLetters({int limit = 50});

  Future<SyncCheckpoint?> readCheckpoint(String collection);

  Future<void> writeCheckpoint(SyncCheckpoint checkpoint);
}

class ConnectivitySnapshot {
  const ConnectivitySnapshot({
    required this.kind,
    required this.isExpensive,
    required this.canReachInternet,
  });

  const ConnectivitySnapshot.offline()
      : kind = ConnectivityKind.offline,
        isExpensive = false,
        canReachInternet = false;

  final ConnectivityKind kind;
  final bool isExpensive;
  final bool canReachInternet;

  bool get isOnline => canReachInternet && kind != ConnectivityKind.offline;
}

abstract interface class ConnectivityMonitor {
  Stream<ConnectivitySnapshot> get changes;

  Future<ConnectivitySnapshot> current();
}

class AlwaysOnlineConnectivityMonitor implements ConnectivityMonitor {
  const AlwaysOnlineConnectivityMonitor();

  @override
  Stream<ConnectivitySnapshot> get changes => const Stream<ConnectivitySnapshot>.empty();

  @override
  Future<ConnectivitySnapshot> current() async {
    return const ConnectivitySnapshot(
      kind: ConnectivityKind.wifi,
      isExpensive: false,
      canReachInternet: true,
    );
  }
}

class RetryPolicy {
  const RetryPolicy({
    this.maxAttempts = 8,
    this.initialDelay = const Duration(milliseconds: 500),
    this.maxDelay = const Duration(minutes: 5),
    this.jitterFactor = 0.2,
    Random? random,
  }) : _random = random;

  final int maxAttempts;
  final Duration initialDelay;
  final Duration maxDelay;
  final double jitterFactor;
  final Random? _random;

  bool shouldRetry(Mutation mutation) => mutation.attemptCount < maxAttempts;

  DateTime nextAttempt(DateTime now, int attempt) {
    final exponent = max(0, attempt);
    final baseMilliseconds = initialDelay.inMilliseconds * pow(2, exponent);
    final capped = min(baseMilliseconds, maxDelay.inMilliseconds).toInt();
    final random = _random ?? Random();
    final jitter = capped * jitterFactor * (random.nextDouble() - 0.5) * 2;
    return now.add(Duration(milliseconds: max(0, capped + jitter).round()));
  }
}

class ConflictContext {
  const ConflictContext({
    required this.local,
    required this.remote,
    required this.mutation,
  });

  final SyncRecord local;
  final SyncRecord remote;
  final Mutation? mutation;
}

class ConflictResolver {
  const ConflictResolver({
    this.strategy = ConflictStrategy.lastWriteWins,
    this.resolver,
  });

  final ConflictStrategy strategy;
  final ConflictMerge? resolver;

  Future<SyncRecord> resolve(ConflictContext context) async {
    final local = context.local;
    final remote = context.remote;
    final resolvedData = switch (strategy) {
      ConflictStrategy.serverWins => remote.data,
      ConflictStrategy.clientWins => local.data,
      ConflictStrategy.mergeFields => <String, Object?>{
          ...remote.data,
          ...local.data,
        },
      ConflictStrategy.manual => await _manual(context),
      ConflictStrategy.timestamp => local.updatedAt.isAfter(remote.updatedAt)
          ? local.data
          : remote.data,
      ConflictStrategy.version => local.version >= remote.version ? local.data : remote.data,
      ConflictStrategy.lastWriteWins => local.updatedAt.isAfter(remote.updatedAt)
          ? local.data
          : remote.data,
    };

    return SyncRecord(
      collection: local.collection,
      id: local.id,
      data: resolvedData,
      version: max(local.version, remote.version) + 1,
      updatedAt: local.updatedAt.isAfter(remote.updatedAt) ? local.updatedAt : remote.updatedAt,
      isDeleted: local.isDeleted && remote.isDeleted,
      isPending: true,
      vector: mergeVectors(local.vector, remote.vector),
      metadata: <String, Object?>{
        ...remote.metadata,
        ...local.metadata,
        'conflictResolvedBy': strategy.name,
      },
    );
  }

  Future<JsonMap> _manual(ConflictContext context) async {
    final callback = resolver;
    if (callback == null) {
      throw const SyncException('Manual conflict strategy requires a resolver.');
    }
    return callback(context);
  }
}

class DeltaPatch {
  const DeltaPatch({
    required this.collection,
    required this.id,
    required this.changedFields,
    required this.values,
    required this.baseVersion,
  });

  final String collection;
  final String id;
  final Set<String> changedFields;
  final JsonMap values;
  final int? baseVersion;

  static DeltaPatch between({
    required SyncRecord? before,
    required SyncRecord after,
  }) {
    final changed = <String>{};
    final values = <String, Object?>{};
    final previous = before?.data ?? const <String, Object?>{};

    for (final entry in after.data.entries) {
      if (!previous.containsKey(entry.key) || previous[entry.key] != entry.value) {
        changed.add(entry.key);
        values[entry.key] = entry.value;
      }
    }

    for (final key in previous.keys) {
      if (!after.data.containsKey(key)) {
        changed.add(key);
        values[key] = null;
      }
    }

    return DeltaPatch(
      collection: after.collection,
      id: after.id,
      changedFields: changed,
      values: values,
      baseVersion: before?.version,
    );
  }
}

Map<String, int> mergeVectors(Map<String, int> left, Map<String, int> right) {
  final merged = <String, int>{...left};
  for (final entry in right.entries) {
    merged[entry.key] = max(merged[entry.key] ?? 0, entry.value);
  }
  return merged;
}

class SyncPullResult {
  const SyncPullResult({
    required this.records,
    required this.checkpoint,
  });

  final List<SyncRecord> records;
  final SyncCheckpoint? checkpoint;
}

class MutationAcknowledgement {
  const MutationAcknowledgement({
    required this.mutationId,
    this.serverRecord,
    this.error,
    this.conflict,
  });

  final String mutationId;
  final SyncRecord? serverRecord;
  final String? error;
  final SyncRecord? conflict;

  bool get isSuccess => error == null && conflict == null;
}

class SyncPushResult {
  const SyncPushResult({required this.acknowledgements});

  final List<MutationAcknowledgement> acknowledgements;
}

abstract interface class SyncTransport {
  Future<SyncPullResult> pullDeltas({
    required String collection,
    SyncCheckpoint? checkpoint,
    int limit = 500,
  });

  Future<SyncPushResult> pushMutations(List<Mutation> mutations);
}

class NoopSyncTransport implements SyncTransport {
  const NoopSyncTransport();

  @override
  Future<SyncPullResult> pullDeltas({
    required String collection,
    SyncCheckpoint? checkpoint,
    int limit = 500,
  }) async {
    return SyncPullResult(records: const <SyncRecord>[], checkpoint: checkpoint);
  }

  @override
  Future<SyncPushResult> pushMutations(List<Mutation> mutations) async {
    return SyncPushResult(
      acknowledgements: mutations
          .map((mutation) => MutationAcknowledgement(mutationId: mutation.id))
          .toList(growable: false),
    );
  }
}

class SignedRequestContext {
  const SignedRequestContext({
    required this.method,
    required this.path,
    required this.body,
    required this.token,
  });

  final String method;
  final String path;
  final JsonMap body;
  final String? token;
}

class SecureSyncConfig {
  const SecureSyncConfig({
    this.tokenProvider,
    this.requestSigner,
    this.encrypt,
    this.decrypt,
  });

  final TokenProvider? tokenProvider;
  final RequestSigner? requestSigner;
  final EncryptionCodec? encrypt;
  final EncryptionCodec? decrypt;
}

class RealtimeMessage {
  const RealtimeMessage({
    required this.channel,
    required this.type,
    required this.payload,
    required this.timestamp,
  });

  final String channel;
  final String type;
  final JsonMap payload;
  final DateTime timestamp;
}

abstract interface class RealtimeConnector {
  Stream<RealtimeConnectionState> get connectionState;

  Stream<RealtimeMessage> get messages;

  Future<void> connect();

  Future<void> disconnect();

  Future<void> subscribe(String channel);

  Future<void> unsubscribe(String channel);

  Future<void> publish(RealtimeMessage message);
}

abstract interface class BackgroundSyncScheduler {
  Future<void> registerPeriodicSync({
    required String taskId,
    required Duration frequency,
  });

  Future<void> cancel(String taskId);
}

class SyncState {
  const SyncState({
    required this.status,
    required this.pendingMutations,
    required this.deadLetterCount,
    required this.lastSyncAt,
    this.message,
  });

  const SyncState.initial()
      : status = SyncStatus.idle,
        pendingMutations = 0,
        deadLetterCount = 0,
        lastSyncAt = null,
        message = null;

  final SyncStatus status;
  final int pendingMutations;
  final int deadLetterCount;
  final DateTime? lastSyncAt;
  final String? message;

  SyncState copyWith({
    SyncStatus? status,
    int? pendingMutations,
    int? deadLetterCount,
    DateTime? lastSyncAt,
    String? message,
  }) {
    return SyncState(
      status: status ?? this.status,
      pendingMutations: pendingMutations ?? this.pendingMutations,
      deadLetterCount: deadLetterCount ?? this.deadLetterCount,
      lastSyncAt: lastSyncAt ?? this.lastSyncAt,
      message: message,
    );
  }
}

class SyncEvent {
  const SyncEvent({
    required this.type,
    required this.message,
    required this.timestamp,
    this.metadata = const <String, Object?>{},
  });

  final String type;
  final String message;
  final DateTime timestamp;
  final JsonMap metadata;
}

class SyncEngine {
  SyncEngine({
    required this.storage,
    SyncTransport? transport,
    ConnectivityMonitor? connectivity,
    ConflictResolver? conflictResolver,
    RetryPolicy? retryPolicy,
    RealtimeConnector? realtime,
    SecureSyncConfig? security,
    Clock? clock,
    this.clientId = 'default-client',
    this.batchSize = 100,
    this.autoSync = true,
  })  : transport = transport ?? const NoopSyncTransport(),
        connectivity = connectivity ?? const AlwaysOnlineConnectivityMonitor(),
        conflictResolver = conflictResolver ?? const ConflictResolver(),
        retryPolicy = retryPolicy ?? const RetryPolicy(),
        realtime = realtime,
        security = security ?? const SecureSyncConfig(),
        clock = clock ?? const Clock();

  final StorageAdapter storage;
  final SyncTransport transport;
  final ConnectivityMonitor connectivity;
  final ConflictResolver conflictResolver;
  final RetryPolicy retryPolicy;
  final RealtimeConnector? realtime;
  final SecureSyncConfig security;
  final Clock clock;
  final String clientId;
  final int batchSize;
  final bool autoSync;

  final StreamController<SyncState> _stateController =
      StreamController<SyncState>.broadcast();
  final StreamController<SyncEvent> _eventController =
      StreamController<SyncEvent>.broadcast();
  final Set<String> _knownCollections = <String>{};
  SyncState _state = const SyncState.initial();
  StreamSubscription<ConnectivitySnapshot>? _connectivitySubscription;
  StreamSubscription<RealtimeMessage>? _realtimeSubscription;
  Future<void>? _activeSync;
  bool _isInitialized = false;
  bool _isPaused = false;

  SyncState get state => _state;

  Stream<SyncState> get states => _stateController.stream;

  Stream<SyncEvent> get events => _eventController.stream;

  Future<void> initialize() async {
    if (_isInitialized) {
      return;
    }
    await storage.initialize();
    _connectivitySubscription = connectivity.changes.listen(_handleConnectivity);
    _realtimeSubscription = realtime?.messages.listen(_handleRealtimeMessage);
    await realtime?.connect();
    _isInitialized = true;

    final snapshot = await connectivity.current();
    _emitState(_state.copyWith(status: snapshot.isOnline ? SyncStatus.idle : SyncStatus.offline));
    if (autoSync && snapshot.isOnline) {
      scheduleSync();
    }
  }

  SyncCollection collection(String name) {
    _knownCollections.add(name);
    return SyncCollection._(engine: this, name: name);
  }

  Future<void> pause() async {
    _isPaused = true;
    _emitState(_state.copyWith(status: SyncStatus.paused));
  }

  Future<void> resume() async {
    _isPaused = false;
    _emitState(_state.copyWith(status: SyncStatus.idle));
    scheduleSync();
  }

  void scheduleSync() {
    if (!autoSync || _isPaused) {
      return;
    }
    _activeSync ??= synchronize().whenComplete(() => _activeSync = null);
  }

  Future<void> synchronize({Iterable<String>? collections}) async {
    if (_isPaused) {
      _emitState(_state.copyWith(status: SyncStatus.paused));
      return;
    }

    final snapshot = await connectivity.current();
    if (!snapshot.isOnline) {
      _emitState(_state.copyWith(status: SyncStatus.offline));
      return;
    }

    _emitState(_state.copyWith(status: SyncStatus.syncing, message: null));
    try {
      final targetCollections = collections ?? _knownCollections;
      for (final collection in targetCollections) {
        await _pullCollection(collection);
      }
      await _flushMutationQueue();
      await _refreshCounters(status: SyncStatus.idle, lastSyncAt: clock.now());
      _emitEvent('sync.completed', 'Synchronization completed.');
    } on Object catch (error) {
      await _refreshCounters(status: SyncStatus.failed, message: error.toString());
      _emitEvent('sync.failed', 'Synchronization failed.', <String, Object?>{
        'error': error.toString(),
      });
    }
  }

  Future<void> close() async {
    await _connectivitySubscription?.cancel();
    await _realtimeSubscription?.cancel();
    await realtime?.disconnect();
    await _stateController.close();
    await _eventController.close();
    await storage.close();
  }

  Future<void> _pullCollection(String collection) async {
    final checkpoint = await storage.readCheckpoint(collection);
    final result = await transport.pullDeltas(
      collection: collection,
      checkpoint: checkpoint,
      limit: batchSize,
    );

    for (final remote in result.records) {
      final local = await storage.getRecord(collection, remote.id);
      if (local != null && local.isPending) {
        final resolved = await conflictResolver.resolve(
          ConflictContext(local: local, remote: remote, mutation: null),
        );
        await storage.upsertRecord(resolved);
        continue;
      }
      await storage.upsertRecord(remote.copyWith(isPending: false));
    }

    final nextCheckpoint = result.checkpoint;
    if (nextCheckpoint != null) {
      await storage.writeCheckpoint(nextCheckpoint);
    }
  }

  Future<void> _flushMutationQueue() async {
    final mutations = await storage.pendingMutations(limit: batchSize, now: clock.now());
    if (mutations.isEmpty) {
      return;
    }

    final result = await transport.pushMutations(mutations);
    final acknowledgements = {
      for (final acknowledgement in result.acknowledgements)
        acknowledgement.mutationId: acknowledgement,
    };

    for (final mutation in mutations) {
      final acknowledgement = acknowledgements[mutation.id];
      if (acknowledgement == null) {
        await _scheduleRetry(mutation, 'Missing server acknowledgement.');
      } else if (acknowledgement.conflict != null) {
        await _resolveMutationConflict(mutation, acknowledgement.conflict!);
      } else if (acknowledgement.error != null) {
        await _scheduleRetry(mutation, acknowledgement.error!);
      } else {
        final serverRecord = acknowledgement.serverRecord;
        if (serverRecord != null) {
          await storage.upsertRecord(serverRecord.copyWith(isPending: false));
        } else {
          final local = await storage.getRecord(mutation.collection, mutation.recordId);
          if (local != null) {
            await storage.upsertRecord(local.copyWith(isPending: false));
          }
        }
        await storage.markMutationSynced(mutation.id);
      }
    }
  }

  Future<void> _resolveMutationConflict(Mutation mutation, SyncRecord remote) async {
    final local = await storage.getRecord(mutation.collection, mutation.recordId);
    if (local == null) {
      await storage.upsertRecord(remote.copyWith(isPending: false));
      await storage.markMutationSynced(mutation.id);
      return;
    }

    final resolved = await conflictResolver.resolve(
      ConflictContext(local: local, remote: remote, mutation: mutation),
    );
    await storage.upsertRecord(resolved);
    await storage.markMutationSynced(mutation.id);
    await storage.enqueueMutation(
      mutation.copyWith(
        attemptCount: 0,
        nextAttemptAt: clock.now(),
        metadata: <String, Object?>{
          ...mutation.metadata,
          'conflictResolution': 'enqueued',
        },
      ),
    );
  }

  Future<void> _scheduleRetry(Mutation mutation, String reason) async {
    if (!retryPolicy.shouldRetry(mutation)) {
      await storage.moveToDeadLetter(mutation, reason);
      return;
    }
    final nextAttempt = retryPolicy.nextAttempt(clock.now(), mutation.attemptCount);
    await storage.updateMutation(
      mutation.copyWith(
        attemptCount: mutation.attemptCount + 1,
        nextAttemptAt: nextAttempt,
        lastError: reason,
      ),
    );
  }

  Future<void> _refreshCounters({
    required SyncStatus status,
    DateTime? lastSyncAt,
    String? message,
  }) async {
    final pending = await storage.pendingMutations(limit: 1000000);
    final dead = await storage.deadLetters(limit: 1000000);
    _emitState(
      SyncState(
        status: status,
        pendingMutations: pending.length,
        deadLetterCount: dead.length,
        lastSyncAt: lastSyncAt ?? _state.lastSyncAt,
        message: message,
      ),
    );
  }

  void _handleConnectivity(ConnectivitySnapshot snapshot) {
    if (!snapshot.isOnline) {
      _emitState(_state.copyWith(status: SyncStatus.offline));
      return;
    }
    if (_state.status == SyncStatus.offline || _state.status == SyncStatus.failed) {
      _emitState(_state.copyWith(status: SyncStatus.idle));
      scheduleSync();
    }
  }

  void _handleRealtimeMessage(RealtimeMessage message) {
    _emitEvent('realtime.${message.type}', 'Realtime message received.', message.payload);
    if (message.type == 'delta' || message.type == 'invalidate') {
      scheduleSync();
    }
  }

  void _emitState(SyncState state) {
    _state = state;
    if (!_stateController.isClosed) {
      _stateController.add(state);
    }
  }

  void _emitEvent(String type, String message, [JsonMap metadata = const <String, Object?>{}]) {
    if (!_eventController.isClosed) {
      _eventController.add(
        SyncEvent(
          type: type,
          message: message,
          timestamp: clock.now(),
          metadata: metadata,
        ),
      );
    }
  }
}

class SyncCollection {
  SyncCollection._({required SyncEngine engine, required this.name}) : _engine = engine;

  final SyncEngine _engine;
  final String name;

  Future<String> insert(JsonMap data, {String? id}) async {
    final now = _engine.clock.now();
    final recordId = id ?? _newId('tmp');
    final record = SyncRecord(
      collection: name,
      id: recordId,
      data: data,
      version: 1,
      updatedAt: now,
      isPending: true,
      vector: <String, int>{_engine.clientId: 1},
    );
    await _engine.storage.upsertRecord(record);
    await _enqueue(record: record, type: MutationType.insert, snapshot: null);
    _engine.scheduleSync();
    return recordId;
  }

  Future<void> update(String id, JsonMap patch) async {
    final existing = await _engine.storage.getRecord(name, id);
    if (existing == null) {
      throw SyncException('Cannot update missing record $name/$id.');
    }
    final now = _engine.clock.now();
    final nextVector = <String, int>{...existing.vector};
    nextVector[_engine.clientId] = (nextVector[_engine.clientId] ?? 0) + 1;
    final updated = existing.copyWith(
      data: <String, Object?>{...existing.data, ...patch},
      version: existing.version + 1,
      updatedAt: now,
      isPending: true,
      vector: nextVector,
    );
    await _engine.storage.upsertRecord(updated);
    await _enqueue(record: updated, type: MutationType.update, snapshot: existing);
    _engine.scheduleSync();
  }

  Future<void> delete(String id) async {
    final existing = await _engine.storage.getRecord(name, id);
    if (existing == null) {
      return;
    }
    final deleted = existing.copyWith(
      isDeleted: true,
      isPending: true,
      updatedAt: _engine.clock.now(),
      version: existing.version + 1,
    );
    await _engine.storage.upsertRecord(deleted);
    await _enqueue(record: deleted, type: MutationType.delete, snapshot: existing);
    _engine.scheduleSync();
  }

  Future<SyncRecord?> get(String id) => _engine.storage.getRecord(name, id);

  Future<List<SyncRecord>> query({QueryOptions options = const QueryOptions()}) {
    return _engine.storage.query(name, options: options);
  }

  Stream<List<SyncRecord>> watch({QueryOptions options = const QueryOptions()}) {
    return _engine.storage.watchCollection(name, options: options);
  }

  Future<void> rollback(String mutationId) async {
    final pending = await _engine.storage.pendingMutations(limit: 1000000);
    final mutation = pending.where((item) => item.id == mutationId).firstOrNull;
    final snapshot = mutation?.snapshot;
    if (mutation == null || snapshot == null) {
      return;
    }
    await _engine.storage.upsertRecord(snapshot.copyWith(isPending: false));
    await _engine.storage.markMutationSynced(mutation.id);
  }

  Future<void> _enqueue({
    required SyncRecord record,
    required MutationType type,
    required SyncRecord? snapshot,
  }) async {
    final sequence = await _engine.storage.nextSequence();
    final patch = DeltaPatch.between(before: snapshot, after: record);
    await _engine.storage.enqueueMutation(
      Mutation(
        id: _newId('mut'),
        collection: name,
        recordId: record.id,
        type: type,
        payload: type == MutationType.update ? patch.values : record.data,
        changedFields: patch.changedFields,
        sequence: sequence,
        baseVersion: snapshot?.version,
        clientTimestamp: _engine.clock.now(),
        idempotencyKey: '$name/${record.id}/$sequence',
        snapshot: snapshot,
      ),
    );
  }
}

String _newId(String prefix) {
  final random = Random.secure().nextInt(1 << 32).toRadixString(16);
  final timestamp = DateTime.now().microsecondsSinceEpoch.toRadixString(16);
  return '${prefix}_$timestamp$random';
}

extension FirstOrNullExtension<T> on Iterable<T> {
  T? get firstOrNull {
    final iterator = this.iterator;
    if (!iterator.moveNext()) {
      return null;
    }
    return iterator.current;
  }
}

class SyncHttpRequest {
  const SyncHttpRequest({
    required this.method,
    required this.path,
    required this.body,
    required this.headers,
  });

  final String method;
  final String path;
  final JsonMap body;
  final JsonMap headers;
}

typedef SyncHttpHandler = Future<JsonMap> Function(SyncHttpRequest request);

class HttpSyncTransport implements SyncTransport {
  const HttpSyncTransport({
    required this.endpoint,
    required this.send,
    this.security = const SecureSyncConfig(),
  });

  final Uri endpoint;
  final SyncHttpHandler send;
  final SecureSyncConfig security;

  @override
  Future<SyncPullResult> pullDeltas({
    required String collection,
    SyncCheckpoint? checkpoint,
    int limit = 500,
  }) async {
    final response = await _request(
      method: 'POST',
      path: '/sync/pull',
      body: <String, Object?>{
        'collection': collection,
        'cursor': checkpoint?.cursor,
        'limit': limit,
      },
    );
    final records = (response['records'] as List<Object?>? ?? const <Object?>[])
        .whereType<JsonMap>()
        .map(_recordFromJson)
        .toList(growable: false);
    final cursor = response['cursor'] as String?;
    return SyncPullResult(
      records: records,
      checkpoint: cursor == null
          ? checkpoint
          : SyncCheckpoint(collection: collection, cursor: cursor, updatedAt: DateTime.now().toUtc()),
    );
  }

  @override
  Future<SyncPushResult> pushMutations(List<Mutation> mutations) async {
    final response = await _request(
      method: 'POST',
      path: '/sync/push',
      body: <String, Object?>{
        'mutations': mutations.map(_mutationToJson).toList(growable: false),
      },
    );
    final acknowledgements = (response['acknowledgements'] as List<Object?>? ?? const <Object?>[])
        .whereType<JsonMap>()
        .map((item) {
      final record = item['serverRecord'];
      final conflict = item['conflict'];
      return MutationAcknowledgement(
        mutationId: item['mutationId'] as String,
        serverRecord: record is JsonMap ? _recordFromJson(record) : null,
        conflict: conflict is JsonMap ? _recordFromJson(conflict) : null,
        error: item['error'] as String?,
      );
    }).toList(growable: false);
    return SyncPushResult(acknowledgements: acknowledgements);
  }

  Future<JsonMap> _request({
    required String method,
    required String path,
    required JsonMap body,
  }) async {
    final token = await security.tokenProvider?.call();
    final signature = await security.requestSigner?.call(
          SignedRequestContext(
            method: method,
            path: path,
            body: body,
            token: token,
          ),
        ) ??
        const <String, Object?>{};
    return send(
      SyncHttpRequest(
        method: method,
        path: endpoint.resolve(path).toString(),
        body: body,
        headers: <String, Object?>{
          if (token != null) 'authorization': 'Bearer $token',
          ...signature,
        },
      ),
    );
  }
}

JsonMap _mutationToJson(Mutation mutation) {
  return <String, Object?>{
    'id': mutation.id,
    'collection': mutation.collection,
    'recordId': mutation.recordId,
    'type': mutation.type.name,
    'payload': mutation.payload,
    'changedFields': mutation.changedFields.toList(growable: false),
    'sequence': mutation.sequence,
    'baseVersion': mutation.baseVersion,
    'clientTimestamp': mutation.clientTimestamp.toIso8601String(),
    'idempotencyKey': mutation.idempotencyKey,
    'attemptCount': mutation.attemptCount,
  };
}

SyncRecord _recordFromJson(JsonMap json) {
  return SyncRecord(
    collection: json['collection'] as String,
    id: json['id'] as String,
    data: (json['data'] as JsonMap?) ?? const <String, Object?>{},
    version: (json['version'] as num?)?.toInt() ?? 1,
    updatedAt: DateTime.tryParse(json['updatedAt'] as String? ?? '') ?? DateTime.now().toUtc(),
    isDeleted: json['isDeleted'] as bool? ?? false,
    isPending: json['isPending'] as bool? ?? false,
    vector: (json['vector'] as Map<Object?, Object?>? ?? const <Object?, Object?>{})
        .map((key, value) => MapEntry(key.toString(), (value as num).toInt())),
    metadata: (json['metadata'] as JsonMap?) ?? const <String, Object?>{},
  );
}
