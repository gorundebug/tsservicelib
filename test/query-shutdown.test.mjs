import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MessageContext } from '../dist/runtime/context.js';
import { ServiceRuntime } from '../dist/runtime/service-runtime.js';
import { makeEndpointConsumer } from '../dist/dataquery/index.js';

function gate() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

for (const timeout of [false, true]) {
  test(`Query shutdown retains connector through cleanup and EndRequest${timeout ? ' after drain timeout' : ''}`, {timeout: 3000}, async () => {
    const events = [];
    const entered = gate(), releaseOperation = gate(), finalizing = gate(), releaseEnd = gate();
    const runtime = new ServiceRuntime({buildRuntimeStreams() {}, validateRuntimeTopology() {}, log: () => ({warn() {}, error() {}})});
    runtime.register({ category: 'managedDataConnector', name: 'query-client', lifecycle: {
      async start() {}, async startAdmission() {}, async stopAdmission() {}, async stop() { events.push('client-closed'); }
    }});
    const context = new MessageContext();
    await runtime.start(context);
    const stream = {
      runtimeEnvironment: () => ({metrics: () => ({enabled: () => false})}),
      consumeResult: async (_ctx, value) => { events.push(value); },
      errorStream: () => ({async emit() {}}), setQueryConsumer() {}
    };
    const endpoint = makeEndpointConsumer(stream, {
      beginRequest: ctx => ({context: ctx, state: {}}),
      async endRequest(ctx, _stream, _state, _error, out) {
        events.push('end-entered'); finalizing.resolve();
        await releaseEnd.promise;
        await out.out(ctx, 'completion');
      }
    }, async () => {
      entered.resolve();
      await releaseOperation.promise;
      events.push('native-cleanup');
    });
    const request = runtime.tasks().admit(() => endpoint.consume(context, 'request'));
    try {
      await entered.promise;
      let stopped = false;
      let stopping = runtime.stop(context, timeout ? 10 : 2000);
      if (timeout) {
        await assert.rejects(stopping);
        assert.equal(runtime.state(), 'stopping');
        assert.ok(!events.includes('client-closed'));
        stopping = runtime.stop(context, 2000);
      }
      const observed = stopping.then(() => { stopped = true; });
      releaseOperation.resolve();
      await finalizing.promise;
      assert.equal(stopped, false);
      assert.deepEqual(events, ['native-cleanup', 'end-entered']);
      releaseEnd.resolve();
      await request;
      await observed;
      assert.deepEqual(events, ['native-cleanup', 'end-entered', 'completion', 'client-closed']);
      assert.equal(runtime.state(), 'stopped');
    } finally {
      releaseOperation.resolve(); releaseEnd.resolve();
      await request;
      await runtime.stop(context, 1000);
    }
  });
}
