import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MessageContext } from '../dist/runtime/context.js';
import { TestMetrics } from '../dist/runtime/testmetrics/index.js';
import { makeEndpointConsumer } from '../dist/dataquery/index.js';

for (const mode of ['empty', 'results', 'partial error', 'cancel', 'concurrent']) {
 test(`Query metrics: ${mode}`, async () => {
  const metrics = new TestMetrics();
  const labels = { connector: 'database', endpoint: 'rows', operation: 'Rows' };
  const active = () => metrics.gaugeValue('dataquery_endpoint_active_requests', labels);
  const stream = {
   runtimeEnvironment: () => ({ metrics: () => metrics, runtimeConfig: () => ({
    endpointById: () => ({ name: 'rows', idDataConnector: 1, queryOperation: 'Rows' }),
    dataConnectorById: () => ({ name: 'database' })
   }) }), endpointId: () => 2,
   consumeResult() {}, errorStream: () => ({ emit() {} }), setQueryConsumer() {}
  };
  let entered = 0;
  let release; const barrier = new Promise(resolve => { release = resolve; });
  const count = mode === 'concurrent' ? 2 : 1;
  const consumer = makeEndpointConsumer(stream, {
   beginRequest: context => ({context, state: 0}),
   async endRequest(context, _stream, _state, _error, out) {
    assert.ok(active() > 0, 'EndRequest must remain active');
    await out.out(context, 'completion');
   }
  }, async (context, _state, _value, out) => {
   if (mode === 'concurrent') {
    if (++entered === count) { assert.equal(active(), count); release(); }
    await barrier;
   }
   if (mode !== 'empty') await out.out(context, 'row');
   if (mode === 'partial error') throw new Error('storage failure');
  });
  const owner = new AbortController();
  if (mode === 'cancel') owner.abort();
  await Promise.all(Array.from({length: count}, () => consumer.consume(new MessageContext(owner.signal), 1)));
  assert.equal(active(), 0);
  assert.equal(metrics.counterValue('dataquery_endpoint_requests_total', labels), count);
  assert.equal(metrics.counterValue('dataquery_endpoint_results_total', labels), count * (['empty','cancel'].includes(mode) ? 1 : 2));
  assert.equal(metrics.histogramValue('dataquery_endpoint_request_duration_seconds', labels).count, count);
  for (const outcome of ['error', 'cancelled']) assert.equal(
   metrics.counterValue('dataquery_endpoint_failures_total', {...labels, outcome}) ?? 0,
   (mode === 'partial error' && outcome === 'error') || (mode === 'cancel' && outcome === 'cancelled') ? 1 : 0);
 });
}
