import 'package:sync_core/sync_core.dart';
import 'package:sync_realtime/sync_realtime.dart';
import 'package:test/test.dart';

void main() {
  test('in-memory realtime socket echoes published messages', () async {
    final client = WebSocketRealtimeClient(
      endpoint: Uri.parse('ws://localhost'),
      socketFactory: (_) async => InMemoryRealtimeSocket(),
    );

    final received = <RealtimeMessage>[];
    final subscription = client.messages.listen(received.add);
    await client.connect();
    await client.publish(
      RealtimeMessage(
        channel: 'todos',
        type: 'delta',
        payload: const {'id': '1'},
        timestamp: DateTime.utc(2026),
      ),
    );
    await Future<void>.delayed(Duration.zero);

    expect(received.single.type, 'delta');
    await subscription.cancel();
    await client.disconnect();
  });
}
