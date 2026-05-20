# Monetization Architecture

OrbitSync is designed for an open-core business model.

## Open-source core

- sync engine
- storage adapters
- websocket client
- CRDT primitives
- local DevTools widget
- mock server

## Enterprise modules

- hosted sync cloud transport
- tenant isolation and audit logs
- advanced CRDT packs
- fleet analytics dashboard
- SOC2-oriented compliance exports
- usage billing and quotas
- edge sync nodes

The public extension points keep paid modules additive: enterprise features can
supply new `SyncTransport`, `StorageAdapter`, `ConflictResolver`, or
`BackgroundSyncScheduler` implementations without forking the SDK.
