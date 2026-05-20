library sync_realtime;

import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:sync_core/sync_core.dart';

abstract interface class RealtimeSocket {
  Stream<Object?> get stream;

  Future<void> send(Object? data);

  Future<void> close();
}

typedef RealtimeSocketFactory = Future<RealtimeSocket> Function(Uri endpoint);

class RealtimePresence {
  const RealtimePresence({
    required this.userId,
    required this.status,
    required this.updatedAt,
    this.metadata = const <String, Object?>{},
  });

  final String userId;
  final String status;
  final DateTime updatedAt;
  final JsonMap metadata;
}

class WebSocketRealtimeClient implements RealtimeConnector {
  WebSocketRealtimeClient({
    required this.endpoint,
    required this.socketFactory,
    this.heartbeatInterval = const Duration(seconds: 25),
    this.reconnectPolicy = const RetryPolicy(
      initialDelay: Duration(milliseconds: 400),
      maxDelay: Duration(seconds: 30),
    ),
  });

  final Uri endpoint;
  final RealtimeSocketFactory socketFactory;
  final Duration heartbeatInterval;
  final RetryPolicy reconnectPolicy;

  final StreamController<RealtimeConnectionState> _connectionState =
      StreamController<RealtimeConnectionState>.broadcast();
  final StreamController<RealtimeMessage> _messages =
      StreamController<RealtimeMessage>.broadcast();
  final Set<String> _subscriptions = <String>{};
  RealtimeSocket? _socket;
  StreamSubscription<Object?>? _socketSubscription;
  Timer? _heartbeatTimer;
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  bool _closedByUser = false;
  RealtimeConnectionState _state = RealtimeConnectionState.disconnected;

  @override
  Stream<RealtimeConnectionState> get connectionState => _connectionState.stream;

  @override
  Stream<RealtimeMessage> get messages => _messages.stream;

  RealtimeConnectionState get state => _state;

  @override
  Future<void> connect() async {
    if (_state == RealtimeConnectionState.connected ||
        _state == RealtimeConnectionState.connecting) {
      return;
    }
    _closedByUser = false;
    _emitState(
      _reconnectAttempts == 0
          ? RealtimeConnectionState.connecting
          : RealtimeConnectionState.reconnecting,
    );

    try {
      final socket = await socketFactory(endpoint);
      _socket = socket;
      await _socketSubscription?.cancel();
      _socketSubscription = socket.stream.listen(
        _handleFrame,
        onError: (Object error) => _scheduleReconnect(error),
        onDone: () => _scheduleReconnect('socket closed'),
      );
      _reconnectAttempts = 0;
      _emitState(RealtimeConnectionState.connected);
      _startHeartbeat();
      await _restoreSubscriptions();
    } on Object catch (error) {
      _scheduleReconnect(error);
    }
  }

  @override
  Future<void> disconnect() async {
    _closedByUser = true;
    _heartbeatTimer?.cancel();
    _reconnectTimer?.cancel();
    await _socketSubscription?.cancel();
    await _socket?.close();
    _emitState(RealtimeConnectionState.disconnected);
  }

  @override
  Future<void> subscribe(String channel) async {
    _subscriptions.add(channel);
    await _sendControl('subscribe', <String, Object?>{'channel': channel});
  }

  @override
  Future<void> unsubscribe(String channel) async {
    _subscriptions.remove(channel);
    await _sendControl('unsubscribe', <String, Object?>{'channel': channel});
  }

  @override
  Future<void> publish(RealtimeMessage message) async {
    await _send(<String, Object?>{
      'type': message.type,
      'channel': message.channel,
      'payload': message.payload,
      'timestamp': message.timestamp.toIso8601String(),
    });
  }

  Future<void> updatePresence(RealtimePresence presence) async {
    await _sendControl('presence', <String, Object?>{
      'userId': presence.userId,
      'status': presence.status,
      'updatedAt': presence.updatedAt.toIso8601String(),
      'metadata': presence.metadata,
    });
  }

  void _handleFrame(Object? frame) {
    final decoded = switch (frame) {
      String value => jsonDecode(value),
      Map<Object?, Object?> value => value,
      _ => null,
    };
    if (decoded is! Map<Object?, Object?>) {
      return;
    }
    final type = decoded['type']?.toString() ?? 'message';
    if (type == 'pong') {
      return;
    }
    _messages.add(
      RealtimeMessage(
        channel: decoded['channel']?.toString() ?? 'global',
        type: type,
        payload: _jsonMap(decoded['payload']),
        timestamp: DateTime.tryParse(decoded['timestamp']?.toString() ?? '') ??
            DateTime.now().toUtc(),
      ),
    );
  }

  Future<void> _restoreSubscriptions() async {
    for (final channel in _subscriptions) {
      await _sendControl('subscribe', <String, Object?>{'channel': channel});
    }
  }

  void _startHeartbeat() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = Timer.periodic(heartbeatInterval, (_) {
      unawaited(
        _sendControl('ping', <String, Object?>{
          'sentAt': DateTime.now().toUtc().toIso8601String(),
        }),
      );
    });
  }

  Future<void> _sendControl(String type, JsonMap payload) {
    return _send(<String, Object?>{
      'type': type,
      'channel': '_control',
      'payload': payload,
      'timestamp': DateTime.now().toUtc().toIso8601String(),
    });
  }

  Future<void> _send(JsonMap frame) async {
    final socket = _socket;
    if (socket == null || _state != RealtimeConnectionState.connected) {
      return;
    }
    await socket.send(jsonEncode(frame));
  }

  void _scheduleReconnect(Object error) {
    if (_closedByUser) {
      return;
    }
    _heartbeatTimer?.cancel();
    _emitState(RealtimeConnectionState.reconnecting);
    final delay = reconnectPolicy.nextAttempt(DateTime.now().toUtc(), _reconnectAttempts);
    _reconnectAttempts += 1;
    _reconnectTimer?.cancel();
    final wait = delay.difference(DateTime.now().toUtc());
    _reconnectTimer = Timer(wait.isNegative ? Duration.zero : wait, () {
      unawaited(connect());
    });
  }

  void _emitState(RealtimeConnectionState state) {
    _state = state;
    if (!_connectionState.isClosed) {
      _connectionState.add(state);
    }
  }
}

class SocketIoRealtimeClient extends WebSocketRealtimeClient {
  SocketIoRealtimeClient({
    required super.endpoint,
    required super.socketFactory,
    super.heartbeatInterval,
    super.reconnectPolicy,
  });
}

class InMemoryRealtimeSocket implements RealtimeSocket {
  InMemoryRealtimeSocket();

  final StreamController<Object?> _controller = StreamController<Object?>.broadcast();

  @override
  Stream<Object?> get stream => _controller.stream;

  @override
  Future<void> send(Object? data) async {
    _controller.add(data);
  }

  @override
  Future<void> close() async {
    await _controller.close();
  }
}

JsonMap _jsonMap(Object? value) {
  if (value is Map<String, Object?>) {
    return value;
  }
  if (value is Map<Object?, Object?>) {
    return value.map((key, entry) => MapEntry(key.toString(), _jsonValue(entry)));
  }
  return const <String, Object?>{};
}

Object? _jsonValue(Object? value) {
  if (value is Map<Object?, Object?>) {
    return value.map((key, entry) => MapEntry(key.toString(), _jsonValue(entry)));
  }
  if (value is List<Object?>) {
    return value.map(_jsonValue).toList(growable: false);
  }
  return value;
}

class BackoffReconnectPlanner {
  const BackoffReconnectPlanner({this.random});

  final Random? random;

  Duration delayForAttempt(int attempt) {
    final policy = RetryPolicy(random: random);
    final now = DateTime.now().toUtc();
    return policy.nextAttempt(now, attempt).difference(now);
  }
}
