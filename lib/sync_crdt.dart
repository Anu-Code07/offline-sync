library sync_crdt;

import 'dart:math';

typedef JsonMap = Map<String, Object?>;

enum ClockComparison { before, after, equal, concurrent }

class VectorClock {
  const VectorClock([this.entries = const <String, int>{}]);

  final Map<String, int> entries;

  VectorClock tick(String nodeId) {
    return VectorClock(<String, int>{
      ...entries,
      nodeId: (entries[nodeId] ?? 0) + 1,
    });
  }

  VectorClock merge(VectorClock other) {
    final merged = <String, int>{...entries};
    for (final entry in other.entries.entries) {
      merged[entry.key] = max(entry.value, merged[entry.key] ?? 0);
    }
    return VectorClock(merged);
  }

  ClockComparison compare(VectorClock other) {
    var hasGreater = false;
    var hasLess = false;
    final keys = <String>{...entries.keys, ...other.entries.keys};

    for (final key in keys) {
      final left = entries[key] ?? 0;
      final right = other.entries[key] ?? 0;
      hasGreater = hasGreater || left > right;
      hasLess = hasLess || left < right;
    }

    if (hasGreater && hasLess) {
      return ClockComparison.concurrent;
    }
    if (hasGreater) {
      return ClockComparison.after;
    }
    if (hasLess) {
      return ClockComparison.before;
    }
    return ClockComparison.equal;
  }

  JsonMap toJson() => <String, Object?>{'entries': entries};
}

class LamportClock {
  const LamportClock({required this.nodeId, this.value = 0});

  final String nodeId;
  final int value;

  LamportClock tick() => LamportClock(nodeId: nodeId, value: value + 1);

  LamportClock observe(int remoteValue) {
    return LamportClock(nodeId: nodeId, value: max(value, remoteValue) + 1);
  }
}

class LwwRegister<T> {
  const LwwRegister({
    required this.value,
    required this.timestamp,
    required this.nodeId,
  });

  final T value;
  final DateTime timestamp;
  final String nodeId;

  LwwRegister<T> merge(LwwRegister<T> other) {
    final timestampComparison = timestamp.compareTo(other.timestamp);
    if (timestampComparison > 0) {
      return this;
    }
    if (timestampComparison < 0) {
      return other;
    }
    return nodeId.compareTo(other.nodeId) >= 0 ? this : other;
  }
}

class OrSet<T> {
  const OrSet({
    this.adds = const <T, Set<String>>{},
    this.removes = const <T, Set<String>>{},
  });

  final Map<T, Set<String>> adds;
  final Map<T, Set<String>> removes;

  Set<T> get values {
    return adds.entries
        .where((entry) {
          final tombstones = removes[entry.key] ?? const <String>{};
          return entry.value.any((tag) => !tombstones.contains(tag));
        })
        .map((entry) => entry.key)
        .toSet();
  }

  OrSet<T> add(T value, String tag) {
    return OrSet<T>(
      adds: _copyWithTag(adds, value, tag),
      removes: removes,
    );
  }

  OrSet<T> remove(T value) {
    final observedTags = adds[value] ?? const <String>{};
    return OrSet<T>(
      adds: adds,
      removes: <T, Set<String>>{
        ...removes,
        value: <String>{...(removes[value] ?? const <String>{}), ...observedTags},
      },
    );
  }

  OrSet<T> merge(OrSet<T> other) {
    return OrSet<T>(
      adds: _mergeTagged(adds, other.adds),
      removes: _mergeTagged(removes, other.removes),
    );
  }
}

class PnCounter {
  const PnCounter({
    this.positive = const <String, int>{},
    this.negative = const <String, int>{},
  });

  final Map<String, int> positive;
  final Map<String, int> negative;

  int get value {
    final increments = positive.values.fold<int>(0, (total, item) => total + item);
    final decrements = negative.values.fold<int>(0, (total, item) => total + item);
    return increments - decrements;
  }

  PnCounter increment(String nodeId, [int amount = 1]) {
    return PnCounter(
      positive: <String, int>{...positive, nodeId: (positive[nodeId] ?? 0) + amount},
      negative: negative,
    );
  }

  PnCounter decrement(String nodeId, [int amount = 1]) {
    return PnCounter(
      positive: positive,
      negative: <String, int>{...negative, nodeId: (negative[nodeId] ?? 0) + amount},
    );
  }

  PnCounter merge(PnCounter other) {
    return PnCounter(
      positive: _mergeCounters(positive, other.positive),
      negative: _mergeCounters(negative, other.negative),
    );
  }
}

class TextAtom {
  const TextAtom({
    required this.id,
    required this.value,
    required this.previousId,
    required this.isDeleted,
  });

  final String id;
  final String value;
  final String? previousId;
  final bool isDeleted;
}

class RgaText {
  const RgaText({this.atoms = const <String, TextAtom>{}});

  final Map<String, TextAtom> atoms;

  String get value {
    final ordered = _orderedAtoms().where((atom) => !atom.isDeleted);
    return ordered.map((atom) => atom.value).join();
  }

  RgaText insert({
    required String id,
    required String value,
    required String? previousId,
  }) {
    return RgaText(
      atoms: <String, TextAtom>{
        ...atoms,
        id: TextAtom(
          id: id,
          value: value,
          previousId: previousId,
          isDeleted: false,
        ),
      },
    );
  }

  RgaText delete(String id) {
    final atom = atoms[id];
    if (atom == null) {
      return this;
    }
    return RgaText(
      atoms: <String, TextAtom>{
        ...atoms,
        id: TextAtom(
          id: atom.id,
          value: atom.value,
          previousId: atom.previousId,
          isDeleted: true,
        ),
      },
    );
  }

  RgaText merge(RgaText other) {
    return RgaText(atoms: <String, TextAtom>{...atoms, ...other.atoms});
  }

  List<TextAtom> _orderedAtoms() {
    final byPrevious = <String?, List<TextAtom>>{};
    for (final atom in atoms.values) {
      byPrevious.putIfAbsent(atom.previousId, () => <TextAtom>[]).add(atom);
    }

    final result = <TextAtom>[];
    void visit(String? previousId) {
      final children = byPrevious[previousId] ?? const <TextAtom>[];
      children.sort((left, right) => left.id.compareTo(right.id));
      for (final child in children) {
        result.add(child);
        visit(child.id);
      }
    }

    visit(null);
    return result;
  }
}

Map<T, Set<String>> _copyWithTag<T>(Map<T, Set<String>> source, T value, String tag) {
  return <T, Set<String>>{
    ...source,
    value: <String>{...(source[value] ?? const <String>{}), tag},
  };
}

Map<T, Set<String>> _mergeTagged<T>(Map<T, Set<String>> left, Map<T, Set<String>> right) {
  final merged = <T, Set<String>>{...left.map((key, value) => MapEntry(key, {...value}))};
  for (final entry in right.entries) {
    merged[entry.key] = <String>{...(merged[entry.key] ?? const <String>{}), ...entry.value};
  }
  return merged;
}

Map<String, int> _mergeCounters(Map<String, int> left, Map<String, int> right) {
  final merged = <String, int>{...left};
  for (final entry in right.entries) {
    merged[entry.key] = max(entry.value, merged[entry.key] ?? 0);
  }
  return merged;
}
