# Performance Guide

- Keep `SyncEngine.batchSize` bounded for low memory use and smooth UI.
- Use `QueryOptions.limit` and pagination for large collections.
- Treat realtime messages as invalidations so reconnect recovery is checkpointed.
- Prefer field-level patches for high-churn records.
- Move expensive merges to isolates in application code when records are large.
- Use background sync frequencies that respect OS battery heuristics.
- Keep DevTools disabled in release builds.

Target workloads:

- 100k+ local records with paginated reads.
- thousands of queued offline mutations.
- resumable sync after process death.
- reconnect recovery after packet drops or missed websocket events.
