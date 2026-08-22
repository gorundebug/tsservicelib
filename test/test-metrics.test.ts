import assert from "node:assert/strict";
import { test } from "node:test";

import { Context } from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

await test("test metrics preserve scope labels, vectors and observations", () => {
  const metrics = new TestMetrics();
  const scope = metrics.scope("datasource_endpoint", {
    connector: "orders",
    endpoint: "processOrder"
  });
  const context = Context.background();
  const messages = scope.counter("messages_total", "messages");
  const events = scope.counterVec("events_total", "events");
  const active = scope.gauge("active_requests", "active");
  const duration = scope.histogram("request_duration_seconds", "duration");
  let oldest = 0;
  scope.observableFloat64Gauge("pending_oldest_age_seconds", "oldest", () => oldest);

  messages.inc(context);
  messages.add(context, 2);
  events.with({ event: "late_result" }).inc(context);
  active.inc();
  active.add(2);
  active.dec();
  duration.observe(context, 0.25);
  duration.observe(context, 0.75);
  oldest = 1.5;

  const base = { connector: "orders", endpoint: "processOrder" };
  assert.equal(metrics.counterValue("datasource_endpoint_messages_total", base), 3);
  assert.equal(
    metrics.counterValue("datasource_endpoint_events_total", {
      ...base,
      event: "late_result"
    }),
    1
  );
  assert.equal(metrics.gaugeValue("datasource_endpoint_active_requests", base), 2);
  assert.deepEqual(metrics.histogramValue("datasource_endpoint_request_duration_seconds", base), {
    count: 2,
    sum: 1,
    values: [0.25, 0.75]
  });
  assert.equal(
    metrics.observableGaugeValue("datasource_endpoint_pending_oldest_age_seconds", base),
    1.5
  );
});

await test("test metrics reject family conflicts and decreasing counters", () => {
  const metrics = new TestMetrics();
  const scope = metrics.scope("runtime");
  const counter = scope.counter("events_total", "events");
  assert.throws(() => {
    counter.add(Context.background(), -1);
  }, /cannot decrease/);
  assert.throws(() => scope.gauge("events_total", "events"), /already registered as counter/);
  assert.throws(() => scope.counter("events_total", "different help"), /different help/);
});
