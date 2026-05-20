# Sync Algorithms

## Mutation queue

1. Persist the record locally with `isPending=true`.
2. Persist a mutation containing sequence, idempotency key, base version, changed
   fields, and rollback snapshot.
3. Flush pending mutations in sequence order.
4. Mark successful mutations synced, retry transient failures, and move exhausted
   attempts into the dead-letter queue.

## Retry policy

Retries use exponential backoff with jitter:

```text
delay = min(initialDelay * 2^attempt, maxDelay) +/- jitter
```

This avoids thundering herds after connectivity restoration or regional server
recovery.

## Delta sync

`DeltaPatch.between()` compares the previous snapshot with the optimistic local
record and records only changed fields. Servers can persist patch payloads,
validate base versions, and return conflicts only when overlapping fields changed.

## Websocket recovery

Realtime events are treated as invalidations, not as the only source of truth.
After reconnect, the engine pulls from collection checkpoints so dropped packets
or missed websocket messages do not corrupt local state.

## Conflict handling

Built-in policies:

- last write wins
- server wins
- client wins
- merge fields
- manual merge
- timestamp based
- version based

Applications should prefer domain-specific manual resolvers for collaborative
objects where business invariants matter.
