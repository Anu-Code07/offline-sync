# OrbitSync Mock Server

Local Express/WebSocket backend for SDK development.

```bash
npm install
npm start
```

Environment toggles:

- `LATENCY_MS=250`
- `FAILURE_RATE=0.05`
- `PACKET_DROP_RATE=0.1`
- `CONFLICT_RATE=0.25`

Endpoints:

- `GET /health`
- `POST /sync/pull`
- `POST /sync/push`
- `POST /admin/reset`

Realtime clients subscribe to collection-named channels. Mutations broadcast
`delta` events so clients can recover through checkpoint-based pull sync.
