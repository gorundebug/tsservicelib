import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Context,
  NoopMetricsEngine,
  PrometheusMetrics,
  PrometheusMetricsEngine,
  makeServiceMetricsEngine
} from "@gorundebug/tsservicelib/runtime";

await test("Prometheus metrics render canonical scoped families and observations", async () => {
  const metrics = new PrometheusMetrics();
  const first = metrics.scope("datasource_endpoint", {
    connector: "orders",
    endpoint: "processOrder"
  });
  const second = metrics.scope("datasource_endpoint", {
    connector: "inventory",
    endpoint: "processOrderItem"
  });
  const context = Context.background();
  first.counter("messages_total", "Processed messages").inc(context);
  second.counter("messages_total", "Processed messages").add(context, 2);
  first.counterVec("events_total", "Endpoint events").with({ event: "request_error" }).inc(context);
  const active = first.gauge("active_requests", "Active requests");
  active.inc();
  active.dec();
  first
    .histogram("request_duration_seconds", "Request duration", {}, [0.1, 1])
    .observe(context, 0.25);
  let oldest = 1.5;
  first.observableFloat64Gauge("pending_oldest_age_seconds", "Oldest pending", () => oldest);

  const engine = new PrometheusMetricsEngine(metrics);
  let output = await engine.render();
  assert.match(output, /# HELP datasource_endpoint_messages_total Processed messages/);
  assert.match(output, /# TYPE datasource_endpoint_messages_total counter/);
  assert.match(
    output,
    /datasource_endpoint_messages_total\{connector="orders",endpoint="processOrder"\} 1/
  );
  assert.match(
    output,
    /datasource_endpoint_messages_total\{connector="inventory",endpoint="processOrderItem"\} 2/
  );
  assert.match(output, /datasource_endpoint_request_duration_seconds_count[^\n]* 1/);
  assert.match(output, /datasource_endpoint_pending_oldest_age_seconds[^\n]* 1.5/);
  assert.match(output, /# TYPE process_cpu_seconds_total counter/);
  assert.match(output, /# TYPE process_resident_memory_bytes gauge/);
  assert.match(output, /# TYPE nodejs_heap_size_used_bytes gauge/);
  assert.match(output, /# TYPE nodejs_eventloop_lag_seconds gauge/);
  assert.match(output, /# TYPE nodejs_gc_duration_seconds histogram/);

  oldest = 2.25;
  output = await engine.render();
  assert.match(output, /datasource_endpoint_pending_oldest_age_seconds[^\n]* 2.25/);
  assert.match(engine.contentType(), /^text\/plain;/);
  await engine.shutdown(Context.background());
});

await test("standard benchmark flag selects renderable no-op metrics", async () => {
  const engine = makeServiceMetricsEngine({ SERVICELIB_NOOP_METRICS: "1" });
  assert.ok(engine instanceof NoopMetricsEngine);
  assert.equal(engine.metrics().enabled(), false);
  assert.match(await engine.render(), /metrics are disabled/u);
  assert.match(engine.contentType(), /^text\/plain/u);
});

await test("Prometheus metrics reject incompatible family declarations", () => {
  const metrics = new PrometheusMetrics();
  const scope = metrics.scope("runtime", { service: "orders" });
  scope.counter("events_total", "events");
  assert.throws(() => scope.gauge("events_total", "events"), /already registered as counter/);
  assert.throws(
    () => metrics.scope("runtime", { other: "label" }).counter("events_total", "events"),
    /different label names/
  );
  assert.throws(() => scope.counter("events_total", "other help"), /different help/);
});
