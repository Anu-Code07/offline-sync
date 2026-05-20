library sync_storage;

import 'dart:async';
import 'dart:convert';

import 'sync_core.dart';

abstract interface class KeyValueStorageBackend {
  Future<void> initialize();

  Future<void> close();

  Future<Map<String, JsonMap>> readNamespace(String namespace);

  Future<void> put(String namespace, String key, JsonMap value);

  Future<void> delete(String namespace, String key);
}

class InMemoryKeyValueBackend implements KeyValueStorageBackend {
  final Map<String, Map<String, JsonMap>> _boxes = <String, Map<String, JsonMap>>{};

  @override
  Future<void> initialize() async {}

  @override
  Future<void> close() async {}

  @override
  Future<Map<String, JsonMap>> readNamespace(String namespace) async {
    return Map<String, JsonMap>.from(_boxes[namespace] ?? const <String, JsonMap>{});
  }

  @override
  Future<void> put(String namespace, String key, JsonMap value) async {
    _boxes.putIfAbsent(namespace, () => <String, JsonMap>{})[key] = value;
  }

  @override
  Future<void> delete(String namespace, String key) async {
    _boxes[namespace]?.remove(key);
  }
}

class InMemoryStorageAdapter extends KeyValueStorageAdapter {
  InMemoryStorageAdapter() : super(backend: InMemoryKeyValueBackend());
}

class KeyValueStorageAdapter implements StorageAdapter {
  KeyValueStorageAdapter({required this.backend});

  static const String _recordsNamespace = 'records';
  static const String _mutationsNamespace = 'mutations';
  static const String _deadLettersNamespace = 'dead_letters';
  static const String _checkpointsNamespace = 'checkpoints';

  final KeyValueStorageBackend backend;
  final StreamController<void> _changes = StreamController<void>.broadcast();
  final Map<String, SyncRecord> _records = <String, SyncRecord>{};
  final Map<String, Mutation> _mutations = <String, Mutation>{};
  final Map<String, Mutation> _deadLetters = <String, Mutation>{};
  final Map<String, SyncCheckpoint> _checkpoints = <String, SyncCheckpoint>{};
  int _sequence = 0;

  @override
  Future<void> initialize() async {
    await backend.initialize();
    _records
      ..clear()
      ..addAll(
        (await backend.readNamespace(_recordsNamespace)).map(
          (key, value) => MapEntry(key, _recordFromJson(value)),
        ),
      );
    _mutations
      ..clear()
      ..addAll(
        (await backend.readNamespace(_mutationsNamespace)).map(
          (key, value) => MapEntry(key, _mutationFromJson(value)),
        ),
      );
    _deadLetters
      ..clear()
      ..addAll(
        (await backend.readNamespace(_deadLettersNamespace)).map(
          (key, value) => MapEntry(key, _mutationFromJson(value)),
        ),
      );
    _checkpoints
      ..clear()
      ..addAll(
        (await backend.readNamespace(_checkpointsNamespace)).map(
          (key, value) => MapEntry(key, _checkpointFromJson(value)),
        ),
      );
    _sequence = _mutations.values.fold<int>(
      _records.length,
      (current, mutation) => mutation.sequence > current ? mutation.sequence : current,
    );
  }

  @override
  Future<void> close() async {
    await _changes.close();
    await backend.close();
  }

  @override
  Future<void> upsertRecord(SyncRecord record) async {
    final key = _recordKey(record.collection, record.id);
    _records[key] = record;
    await backend.put(_recordsNamespace, key, _recordToJson(record));
    _notify();
  }

  @override
  Future<SyncRecord?> getRecord(String collection, String id) async {
    return _records[_recordKey(collection, id)];
  }

  @override
  Future<void> deleteRecord(String collection, String id, {DateTime? deletedAt}) async {
    final key = _recordKey(collection, id);
    final existing = _records[key];
    if (existing == null) {
      return;
    }
    await upsertRecord(
      existing.copyWith(
        isDeleted: true,
        updatedAt: deletedAt ?? DateTime.now().toUtc(),
      ),
    );
  }

  @override
  Future<List<SyncRecord>> query(
    String collection, {
    QueryOptions options = const QueryOptions(),
  }) async {
    final filtered = _records.values.where((record) {
      if (record.collection != collection) {
        return false;
      }
      if (!options.includeDeleted && record.isDeleted) {
        return false;
      }
      return options.where?.call(record) ?? true;
    }).toList()
      ..sort((left, right) => right.updatedAt.compareTo(left.updatedAt));

    final offset = options.offset < 0
        ? 0
        : options.offset > filtered.length
            ? filtered.length
            : options.offset;
    final requestedEnd = options.limit == null ? filtered.length : offset + options.limit!;
    final end = requestedEnd > filtered.length ? filtered.length : requestedEnd;
    return filtered.sublist(offset, end);
  }

  @override
  Stream<List<SyncRecord>> watchCollection(
    String collection, {
    QueryOptions options = const QueryOptions(),
  }) async* {
    yield await query(collection, options: options);
    await for (final _ in _changes.stream) {
      yield await query(collection, options: options);
    }
  }

  @override
  Future<int> nextSequence() async {
    _sequence += 1;
    return _sequence;
  }

  @override
  Future<void> enqueueMutation(Mutation mutation) async {
    _mutations[mutation.id] = mutation;
    await backend.put(_mutationsNamespace, mutation.id, _mutationToJson(mutation));
    _notify();
  }

  @override
  Future<List<Mutation>> pendingMutations({int limit = 50, DateTime? now}) async {
    final current = now ?? DateTime.now().toUtc();
    final mutations = _mutations.values.where((mutation) {
      final nextAttemptAt = mutation.nextAttemptAt;
      return nextAttemptAt == null || !nextAttemptAt.isAfter(current);
    }).toList()
      ..sort((left, right) => left.sequence.compareTo(right.sequence));
    return mutations.take(limit).toList(growable: false);
  }

  @override
  Future<void> markMutationSynced(String mutationId) async {
    _mutations.remove(mutationId);
    await backend.delete(_mutationsNamespace, mutationId);
    _notify();
  }

  @override
  Future<void> updateMutation(Mutation mutation) async {
    _mutations[mutation.id] = mutation;
    await backend.put(_mutationsNamespace, mutation.id, _mutationToJson(mutation));
    _notify();
  }

  @override
  Future<void> moveToDeadLetter(Mutation mutation, String reason) async {
    final deadLetter = mutation.copyWith(lastError: reason);
    _mutations.remove(mutation.id);
    _deadLetters[mutation.id] = deadLetter;
    await backend.delete(_mutationsNamespace, mutation.id);
    await backend.put(_deadLettersNamespace, mutation.id, _mutationToJson(deadLetter));
    _notify();
  }

  @override
  Future<List<Mutation>> deadLetters({int limit = 50}) async {
    final mutations = _deadLetters.values.toList()
      ..sort((left, right) => left.sequence.compareTo(right.sequence));
    return mutations.take(limit).toList(growable: false);
  }

  @override
  Future<SyncCheckpoint?> readCheckpoint(String collection) async {
    return _checkpoints[collection];
  }

  @override
  Future<void> writeCheckpoint(SyncCheckpoint checkpoint) async {
    _checkpoints[checkpoint.collection] = checkpoint;
    await backend.put(
      _checkpointsNamespace,
      checkpoint.collection,
      _checkpointToJson(checkpoint),
    );
  }

  void _notify() {
    if (!_changes.isClosed) {
      _changes.add(null);
    }
  }
}

abstract interface class SqliteExecutor {
  Future<void> execute(String sql, [List<Object?> arguments = const <Object?>[]]);

  Future<List<Map<String, Object?>>> query(
    String sql, [
    List<Object?> arguments = const <Object?>[],
  ]);
}

class SqliteStorageAdapter extends KeyValueStorageAdapter {
  SqliteStorageAdapter({required SqliteExecutor executor})
      : super(backend: SqliteKeyValueBackend(executor: executor));
}

class SqliteKeyValueBackend implements KeyValueStorageBackend {
  SqliteKeyValueBackend({required this.executor});

  final SqliteExecutor executor;

  @override
  Future<void> initialize() async {
    await executor.execute(
      'CREATE TABLE IF NOT EXISTS orbitsync_store (namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(namespace, key))',
    );
  }

  @override
  Future<void> close() async {}

  @override
  Future<Map<String, JsonMap>> readNamespace(String namespace) async {
    final rows = await executor.query(
      'SELECT key, value FROM orbitsync_store WHERE namespace = ?',
      <Object?>[namespace],
    );
    return <String, JsonMap>{
      for (final row in rows)
        row['key']! as String: jsonDecodeMap(row['value']! as String),
    };
  }

  @override
  Future<void> put(String namespace, String key, JsonMap value) async {
    await executor.execute(
      'INSERT OR REPLACE INTO orbitsync_store(namespace, key, value) VALUES(?, ?, ?)',
      <Object?>[namespace, key, jsonEncodeMap(value)],
    );
  }

  @override
  Future<void> delete(String namespace, String key) async {
    await executor.execute(
      'DELETE FROM orbitsync_store WHERE namespace = ? AND key = ?',
      <Object?>[namespace, key],
    );
  }
}

abstract interface class HiveBoxBridge {
  Future<Map<String, JsonMap>> readBox(String box);

  Future<void> put(String box, String key, JsonMap value);

  Future<void> delete(String box, String key);
}

class HiveStorageAdapter extends KeyValueStorageAdapter {
  HiveStorageAdapter({required HiveBoxBridge hive})
      : super(backend: HiveKeyValueBackend(hive: hive));
}

class HiveKeyValueBackend implements KeyValueStorageBackend {
  HiveKeyValueBackend({required this.hive});

  final HiveBoxBridge hive;

  @override
  Future<void> initialize() async {}

  @override
  Future<void> close() async {}

  @override
  Future<Map<String, JsonMap>> readNamespace(String namespace) => hive.readBox(namespace);

  @override
  Future<void> put(String namespace, String key, JsonMap value) {
    return hive.put(namespace, key, value);
  }

  @override
  Future<void> delete(String namespace, String key) => hive.delete(namespace, key);
}

abstract interface class IsarDocumentBridge {
  Future<Map<String, JsonMap>> readCollection(String collection);

  Future<void> putDocument(String collection, String key, JsonMap value);

  Future<void> deleteDocument(String collection, String key);
}

class IsarStorageAdapter extends KeyValueStorageAdapter {
  IsarStorageAdapter({required IsarDocumentBridge isar})
      : super(backend: IsarKeyValueBackend(isar: isar));
}

class IsarKeyValueBackend implements KeyValueStorageBackend {
  IsarKeyValueBackend({required this.isar});

  final IsarDocumentBridge isar;

  @override
  Future<void> initialize() async {}

  @override
  Future<void> close() async {}

  @override
  Future<Map<String, JsonMap>> readNamespace(String namespace) {
    return isar.readCollection(namespace);
  }

  @override
  Future<void> put(String namespace, String key, JsonMap value) {
    return isar.putDocument(namespace, key, value);
  }

  @override
  Future<void> delete(String namespace, String key) {
    return isar.deleteDocument(namespace, key);
  }
}

String _recordKey(String collection, String id) => '$collection/$id';

JsonMap _recordToJson(SyncRecord record) {
  return <String, Object?>{
    'collection': record.collection,
    'id': record.id,
    'data': record.data,
    'version': record.version,
    'updatedAt': record.updatedAt.toIso8601String(),
    'isDeleted': record.isDeleted,
    'isPending': record.isPending,
    'vector': record.vector,
    'metadata': record.metadata,
  };
}

SyncRecord _recordFromJson(JsonMap json) {
  return SyncRecord(
    collection: json['collection']! as String,
    id: json['id']! as String,
    data: (json['data'] as JsonMap?) ?? const <String, Object?>{},
    version: (json['version'] as num?)?.toInt() ?? 1,
    updatedAt: DateTime.parse(json['updatedAt']! as String),
    isDeleted: json['isDeleted'] as bool? ?? false,
    isPending: json['isPending'] as bool? ?? false,
    vector: (json['vector'] as Map<Object?, Object?>? ?? const <Object?, Object?>{}).map(
      (key, value) => MapEntry(key.toString(), (value as num).toInt()),
    ),
    metadata: (json['metadata'] as JsonMap?) ?? const <String, Object?>{},
  );
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
    'snapshot': mutation.snapshot == null ? null : _recordToJson(mutation.snapshot!),
    'attemptCount': mutation.attemptCount,
    'nextAttemptAt': mutation.nextAttemptAt?.toIso8601String(),
    'lastError': mutation.lastError,
    'metadata': mutation.metadata,
  };
}

Mutation _mutationFromJson(JsonMap json) {
  final snapshot = json['snapshot'];
  return Mutation(
    id: json['id']! as String,
    collection: json['collection']! as String,
    recordId: json['recordId']! as String,
    type: MutationType.values.byName(json['type']! as String),
    payload: (json['payload'] as JsonMap?) ?? const <String, Object?>{},
    changedFields: ((json['changedFields'] as List<Object?>?) ?? const <Object?>[])
        .map((field) => field.toString())
        .toSet(),
    sequence: (json['sequence'] as num).toInt(),
    baseVersion: (json['baseVersion'] as num?)?.toInt(),
    clientTimestamp: DateTime.parse(json['clientTimestamp']! as String),
    idempotencyKey: json['idempotencyKey']! as String,
    snapshot: snapshot is JsonMap ? _recordFromJson(snapshot) : null,
    attemptCount: (json['attemptCount'] as num?)?.toInt() ?? 0,
    nextAttemptAt: json['nextAttemptAt'] == null
        ? null
        : DateTime.parse(json['nextAttemptAt']! as String),
    lastError: json['lastError'] as String?,
    metadata: (json['metadata'] as JsonMap?) ?? const <String, Object?>{},
  );
}

JsonMap _checkpointToJson(SyncCheckpoint checkpoint) {
  return <String, Object?>{
    'collection': checkpoint.collection,
    'cursor': checkpoint.cursor,
    'updatedAt': checkpoint.updatedAt.toIso8601String(),
  };
}

SyncCheckpoint _checkpointFromJson(JsonMap json) {
  return SyncCheckpoint(
    collection: json['collection']! as String,
    cursor: json['cursor']! as String,
    updatedAt: DateTime.parse(json['updatedAt']! as String),
  );
}

String jsonEncodeMap(JsonMap value) {
  return jsonEncode(value);
}

JsonMap jsonDecodeMap(String value) {
  final decoded = jsonDecode(value);
  if (decoded is! Map<Object?, Object?>) {
    throw const FormatException('Expected a JSON object.');
  }
  return decoded.map((key, entry) => MapEntry(key.toString(), _normalizeJson(entry)));
}

JsonMap _normalizeJsonMap(Map<String, Object?> value) {
  return value.map((key, entry) => MapEntry(key, _normalizeJson(entry)));
}

Object? _normalizeJson(Object? value) {
  if (value is Map<String, Object?>) {
    return _normalizeJsonMap(value);
  }
  if (value is Map<Object?, Object?>) {
    return value.map((key, entry) => MapEntry(key.toString(), _normalizeJson(entry)));
  }
  if (value is List<Object?>) {
    return value.map(_normalizeJson).toList(growable: false);
  }
  return value;
}
