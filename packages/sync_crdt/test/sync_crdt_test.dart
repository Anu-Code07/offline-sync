import 'package:sync_crdt/sync_crdt.dart';
import 'package:test/test.dart';

void main() {
  test('vector clocks detect concurrency', () {
    final left = const VectorClock().tick('a');
    final right = const VectorClock().tick('b');

    expect(left.compare(right), ClockComparison.concurrent);
  });

  test('or-set merge does not resurrect removed observed tags', () {
    final left = const OrSet<String>().add('milk', 'a1');
    final removed = left.remove('milk');
    final merged = left.merge(removed);

    expect(merged.values, isEmpty);
  });
}
