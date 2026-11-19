import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { MessageContext } from '../dist/runtime/context.js';
import { makeEndpointConsumer } from '../dist/dataquery/index.js';
import { makeRowsEndpointConsumer, makeExecuteEndpointConsumer } from '../dist/dataquery/postgresqlkysely/index.js';

function fixture() {
  const values = [], outcomes = [], order = [];
  const stream = {
    runtimeEnvironment: () => ({ metrics: () => ({ enabled: () => false }) }),
    consumeResult: (_ctx, value) => { values.push(value); },
    errorStream: () => ({ emit() {} }),
    setQueryConsumer() {}
  };
  const handler = {
    beginRequest: context => ({ context, state: 41 }),
    async endRequest(context, _stream, state, error, out) {
      assert.equal(state, 41);
      order.push('end');
      outcomes.push(error);
      await out.out(context, 'done');
    }
  };
  return { stream, handler, values, outcomes, order };
}

for (const [name, parentMs, operationMs] of [['operation', 1000, 20], ['parent', 20, 1000], ['inherited', 20, 0]]) {
  test(`Query uses shorter ${name} deadline and waits for cleanup`, async () => {
    const {stream, handler, values, outcomes, order} = fixture();
    const parentAbort = new AbortController();
    const parent = new MessageContext(parentAbort.signal).bounded(parentMs);
    try {
      const consumer = makeEndpointConsumer(stream, handler, async (context, _state, _value, out) => {
        try {
          await out.out(context, 'partial');
          await delay(2000, undefined, {signal: context.signal()});
        } finally { order.push('closed'); }
      }, {operationTimeoutMs: operationMs});
      await consumer.consume(parent, 'request');
      assert.deepEqual(order, ['closed', 'end']);
      assert.deepEqual(values, ['partial', 'done']);
      assert.equal(outcomes.length, 1);
      assert.ok(outcomes[0] instanceof Error);
      if (name === 'operation') assert.equal(parent.cancelled(), false);
    } finally { parentAbort.abort(); }
  });
}

for (const operation of ['Rows', 'Execute']) {
  test(`PostgreSQL ${operation} bounds request building before touching pool`, async () => {
    const {stream, handler, outcomes} = fixture();
    let poolTouched = false;
    const db = { getExecutor() { poolTouched = true; throw new Error('must not acquire'); } };
    const endpoint = {...handler, async buildQuery(context) {
      await delay(2000, undefined, {signal: context.signal()});
      assert.fail('timed-out builder completed');
    }};
    const consumer = operation === 'Rows'
      ? makeRowsEndpointConsumer(stream, db, endpoint, 256, {operationTimeoutMs: 20})
      : makeExecuteEndpointConsumer(stream, db, endpoint, {operationTimeoutMs: 20});
    await consumer.consume(new MessageContext(), 'request');
    assert.equal(poolTouched, false);
    assert.equal(outcomes.length, 1);
    assert.ok(outcomes[0] instanceof Error);
  });
}

test('Query rejects invalid operation timeouts', () => {
  const {stream, handler} = fixture();
  for (const operationTimeoutMs of [-1, true, 1.5, NaN, Infinity, 2147483648]) {
    assert.throws(() => makeEndpointConsumer(stream, handler, () => {}, {operationTimeoutMs}), /timeout/);
  }
});
