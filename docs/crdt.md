# CRDT Concepts

OrbitSync includes small CRDT primitives in the `sync_crdt.dart` entrypoint for
data types that need automatic, convergent merges.

- `VectorClock` detects causal ordering and concurrent updates.
- `LamportClock` provides monotonic event ordering for mutation metadata.
- `LwwRegister<T>` resolves scalar fields with deterministic timestamp/node
  tie-breaking.
- `OrSet<T>` supports add/remove set semantics without resurrecting deleted
  values after merge.
- `PnCounter` supports distributed increments and decrements.
- `RgaText` demonstrates collaborative text ordering.

These utilities are intentionally composable. The sync engine remains strategy
based, so developers can use CRDTs only where they are appropriate rather than
forcing every record into a single merge model.
