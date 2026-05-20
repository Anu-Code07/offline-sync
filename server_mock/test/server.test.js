import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createMockSyncServer } from '../src/server.js';

test('push then pull returns accepted record', async (t) => {
  const { server } = createMockSyncServer({ latencyMs: 0, failureRate: 0, conflictRate: 0 });
  t.after(() => server.close());

  await listen(server);
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const mutation = {
    id: 'm1',
    collection: 'todos',
    recordId: 'todo-1',
    type: 'insert',
    payload: { title: 'Buy milk' },
    changedFields: ['title'],
    sequence: 1,
    baseVersion: null,
    clientTimestamp: new Date().toISOString(),
    idempotencyKey: 'todos/todo-1/1',
  };

  const push = await fetch(`${baseUrl}/sync/push`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mutations: [mutation] }),
  });
  assert.equal(push.status, 200);

  const pull = await fetch(`${baseUrl}/sync/pull`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ collection: 'todos', cursor: '0' }),
  });
  const body = await pull.json();
  assert.equal(body.records.length, 1);
  assert.equal(body.records[0].data.title, 'Buy milk');
});

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
}
