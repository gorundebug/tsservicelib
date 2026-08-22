import assert from "node:assert/strict";
import { test } from "node:test";

import { Context, DelayPool, PoolStoppedError } from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

await test("delay pool executes once at the earlier context deadline", async () => {
  const pool = new DelayPool();
  let executions = 0;
  await pool.start(Context.background());
  const context = Context.background().bounded(50);
  pool.delay(context, 60_000, () => {
    executions += 1;
  });
  await pool.stop(Context.background());

  assert.equal(executions, 1);
  assert.equal(pool.pendingCount(), 0);
});

await test("delay cancellation executes accepted callback immediately and exactly once", async () => {
  const pool = new DelayPool();
  const controller = new AbortController();
  const context = Context.background().withExternalCancellation(controller.signal);
  let executions = 0;
  await pool.start(context);
  pool.delay(context, 60_000, () => {
    executions += 1;
  });
  controller.abort();
  await pool.stop(Context.background());

  assert.equal(executions, 1);
});

await test("delay pool rejects new work after stop", async () => {
  const pool = new DelayPool({ name: "delay" });
  const context = Context.background();
  await pool.start(context);
  await pool.stop(context);

  assert.throws(() => {
    pool.delay(context, 0, () => undefined);
  }, PoolStoppedError);
});

await test("delay pool accepts work before lazy lifecycle start", async () => {
  const pool = new DelayPool();
  const context = Context.background();
  let executions = 0;
  pool.delay(context, 0, () => {
    executions += 1;
  });
  await pool.start(context);
  await pool.stop(context);
  assert.equal(executions, 1);
});

await test("delay pool records the canonical queue, execution and cancellation metrics", async () => {
  const metrics = new TestMetrics();
  const pool = new DelayPool({ metrics, service: "Service" });
  const controller = new AbortController();
  const context = Context.background().withExternalCancellation(controller.signal);
  await pool.start(Context.background());
  assert.equal(metrics.gaugeValue("delay_pool_wait_queue_length", { service: "Service" }), 0);
  pool.delay(context, 60_000, () => undefined);
  controller.abort(new Error("request complete"));
  await pool.stop(Context.background());

  const labels = { service: "Service" };
  assert.equal(metrics.gaugeValue("delay_pool_wait_queue_length", labels), 0);
  assert.equal(metrics.counterValue("delay_pool_tasks_total", labels), 1);
  assert.equal(
    metrics.counterValue("delay_pool_events_total", {
      ...labels,
      event: "task_cancelled"
    }),
    1
  );
  assert.equal(
    metrics.histogramValue("delay_pool_task_execution_duration_seconds", labels)?.count,
    1
  );
});
