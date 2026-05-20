import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import { z } from 'zod';

const mutationSchema = z.object({
  id: z.string(),
  collection: z.string(),
  recordId: z.string(),
  type: z.enum(['insert', 'update', 'delete']),
  payload: z.record(z.string(), z.unknown()),
  changedFields: z.array(z.string()).default([]),
  sequence: z.number(),
  baseVersion: z.number().nullable().optional(),
  clientTimestamp: z.string(),
  idempotencyKey: z.string(),
});

export function createMockSyncServer(options = {}) {
  const app = express();
  const server = http.createServer(app);
  const wss = new WebSocketServer({ server });
  const records = new Map();
  const idempotency = new Set();
  const rooms = new Map();
  const config = {
    latencyMs: Number(options.latencyMs ?? process.env.LATENCY_MS ?? 75),
    failureRate: Number(options.failureRate ?? process.env.FAILURE_RATE ?? 0),
    packetDropRate: Number(options.packetDropRate ?? process.env.PACKET_DROP_RATE ?? 0),
    conflictRate: Number(options.conflictRate ?? process.env.CONFLICT_RATE ?? 0),
  };

  app.use(cors());
  app.use(express.json({ limit: '2mb' }));
  app.use(async (_request, _response, next) => {
    await sleep(config.latencyMs);
    if (Math.random() < config.failureRate) {
      next(Object.assign(new Error('Injected failure'), { statusCode: 503 }));
      return;
    }
    next();
  });

  app.get('/health', (_request, response) => {
    response.json({ ok: true, config });
  });

  app.post('/sync/pull', (request, response) => {
    const { collection, cursor, limit = 500 } = request.body;
    const since = cursor ? Number(cursor) : 0;
    const changed = [...records.values()]
      .filter((record) => record.collection === collection && record.version > since)
      .sort((left, right) => left.version - right.version)
      .slice(0, limit);
    const nextCursor = changed.at(-1)?.version ?? since;
    response.json({ records: changed, cursor: String(nextCursor) });
  });

  app.post('/sync/push', (request, response) => {
    const parsed = z.object({ mutations: z.array(mutationSchema) }).parse(request.body);
    const acknowledgements = parsed.mutations.map((mutation) => {
      if (idempotency.has(mutation.idempotencyKey)) {
        return { mutationId: mutation.id };
      }
      idempotency.add(mutation.idempotencyKey);

      const key = recordKey(mutation.collection, mutation.recordId);
      const existing = records.get(key);
      const shouldConflict =
        existing &&
        mutation.baseVersion != null &&
        existing.version !== mutation.baseVersion &&
        Math.random() < Math.max(config.conflictRate, 0.1);

      if (shouldConflict) {
        return { mutationId: mutation.id, conflict: existing };
      }

      const version = (existing?.version ?? 0) + 1;
      const now = new Date().toISOString();
      const serverRecord = {
        collection: mutation.collection,
        id: mutation.recordId,
        data:
          mutation.type === 'update'
            ? { ...(existing?.data ?? {}), ...mutation.payload }
            : mutation.payload,
        version,
        updatedAt: now,
        isDeleted: mutation.type === 'delete',
        isPending: false,
        vector: { server: version },
        metadata: { acceptedMutation: mutation.id },
      };
      records.set(key, serverRecord);
      broadcast(mutation.collection, {
        type: 'delta',
        channel: mutation.collection,
        payload: { collection: mutation.collection, id: mutation.recordId },
        timestamp: now,
      });
      return { mutationId: mutation.id, serverRecord };
    });

    response.json({ acknowledgements });
  });

  app.post('/admin/reset', (_request, response) => {
    records.clear();
    idempotency.clear();
    response.json({ ok: true });
  });

  app.use((error, _request, response, _next) => {
    response.status(error.statusCode ?? 500).json({
      error: error.message ?? 'Unexpected server error',
    });
  });

  wss.on('connection', (socket) => {
    const socketId = crypto.randomUUID();
    socket.on('message', (raw) => {
      const message = JSON.parse(raw.toString());
      if (message.type === 'ping') {
        safeSend(socket, { type: 'pong', channel: '_control', payload: {}, timestamp: new Date().toISOString() });
        return;
      }
      if (message.type === 'subscribe') {
        const channel = message.payload?.channel;
        rooms.set(channel, rooms.get(channel) ?? new Set());
        rooms.get(channel).add(socket);
        safeSend(socket, { type: 'subscribed', channel, payload: { socketId }, timestamp: new Date().toISOString() });
        return;
      }
      if (message.type === 'unsubscribe') {
        rooms.get(message.payload?.channel)?.delete(socket);
        return;
      }
      if (message.channel) {
        broadcast(message.channel, message);
      }
    });
    socket.on('close', () => {
      for (const subscribers of rooms.values()) {
        subscribers.delete(socket);
      }
    });
  });

  function broadcast(channel, message) {
    for (const socket of rooms.get(channel) ?? []) {
      if (Math.random() >= config.packetDropRate) {
        safeSend(socket, message);
      }
    }
  }

  return { app, server, wss, records, config };
}

function safeSend(socket, message) {
  if (socket.readyState === 1) {
    socket.send(JSON.stringify(message));
  }
}

function recordKey(collection, id) {
  return `${collection}/${id}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT ?? 8787);
  const { server } = createMockSyncServer();
  server.listen(port, () => {
    console.log(`OrbitSync mock server listening on http://localhost:${port}`);
  });
}
