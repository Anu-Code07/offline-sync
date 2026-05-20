# Troubleshooting

## Queue is not flushing

- Verify connectivity monitor reports `canReachInternet=true`.
- Check retry `nextAttemptAt`; mutations may be backing off.
- Inspect dead letters in `sync_devtools`.

## Conflicts keep repeating

- Ensure the server increments versions after accepting patches.
- Prefer manual merge for records with nested business rules.
- Confirm idempotency keys are deduplicated server-side.

## Realtime reconnects loop

- Check heartbeat intervals and load balancer idle timeouts.
- Confirm room subscription acknowledgements are not treated as errors.
- Use checkpoint pulls after reconnect to recover missed events.

## Large lists rebuild too often

- Paginate local queries.
- Keep widgets dumb and derive presentation state outside `build()`.
- Use collection-specific watches instead of global sync-state listeners.
