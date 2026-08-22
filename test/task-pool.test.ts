import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import {
  Context,
  PoolStoppedError,
  PriorityTaskPool,
  TaskPool
} from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

const taskPoolMetricNames = [
  "task_pool_events_total",
  "task_pool_executors_allocated",
  "task_pool_executors_busy",
  "task_pool_executors_target",
  "task_pool_queue_length",
  "task_pool_task_execution_duration_seconds",
  "task_pool_tasks_total"
] as const;

const priorityTaskPoolMetricNames = [
  "priority_task_pool_events_total",
  "priority_task_pool_executors_allocated",
  "priority_task_pool_executors_busy",
  "priority_task_pool_executors_target",
  "priority_task_pool_queue_length",
  "priority_task_pool_task_execution_duration_seconds",
  "priority_task_pool_tasks_total"
] as const;

await test("task pool bounds concurrent asynchronous work and drains on stop", async () => {
  const context = Context.background();
  const pool = new TaskPool({ name: "default", executorsCount: 2 });
  let active = 0;
  let maximum = 0;
  await pool.start(context);

  for (let index = 0; index < 4; index += 1) {
    pool.addTask(context, async () => {
      active += 1;
      maximum = Math.max(maximum, active);
      await delay(1);
      active -= 1;
    });
  }
  await Promise.resolve();
  assert.equal(maximum, 2);
  assert.equal(pool.activeCount(), 2);
  assert.equal(pool.queueLength(), 2);

  await pool.stop(context);
  assert.equal(active, 0);
  assert.throws(() => {
    pool.addTask(context, () => undefined);
  }, PoolStoppedError);
});

await test("pool work waiting on I/O does not block transports, timers or unrelated work", async () => {
  const server = createServer((_request, response) => {
    response.end("ready");
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const pool = new TaskPool({ name: "io-progress", executorsCount: 1 });
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let unrelatedProgress = false;
  let timerProgress = false;
  try {
    await pool.start(Context.background());
    pool.addTask(Context.background(), () => blocked);
    queueMicrotask(() => {
      unrelatedProgress = true;
    });
    setTimeout(() => {
      timerProgress = true;
    }, 1).unref();

    const address = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${String(address.port)}/ready`);
    assert.equal(await response.text(), "ready");
    await delay(5);
    assert.equal(pool.activeCount(), 1);
    assert.equal(unrelatedProgress, true);
    assert.equal(timerProgress, true);
  } finally {
    release();
    await pool.stop(Context.background());
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error === undefined) resolve();
        else reject(error);
      });
    });
  }
});

await test("priority pool uses canonical lower-number-first ordering", async () => {
  const context = Context.background();
  const pool = new PriorityTaskPool({ name: "priority", executorsCount: 1 });
  const order: number[] = [];
  pool.addTask(context, 10, () => {
    order.push(10);
  });
  pool.addTask(context, -2, () => {
    order.push(-2);
  });
  pool.addTask(context, 3, () => {
    order.push(3);
  });

  await pool.start(context);
  await pool.stop(context);
  assert.deepEqual(order, [-2, 3, 10]);
});

await test("priority pool preserves FIFO order for equal priorities", async () => {
  const context = Context.background();
  const pool = new PriorityTaskPool({ name: "priority-fifo", executorsCount: 1 });
  const order: number[] = [];
  for (const value of [1, 2, 3]) {
    pool.addTask(context, 0, () => {
      order.push(value);
    });
  }

  await pool.start(context);
  await pool.stop(context);
  assert.deepEqual(order, [1, 2, 3]);
});

await test("task pool hot resize increases concurrency without dropping queued work", async () => {
  const context = Context.background();
  const pool = new TaskPool({ name: "resize", executorsCount: 1 });
  let active = 0;
  let maximum = 0;
  await pool.start(context);
  for (let index = 0; index < 3; index += 1) {
    pool.addTask(context, async () => {
      active += 1;
      maximum = Math.max(maximum, active);
      await delay(5);
      active -= 1;
    });
  }
  await Promise.resolve();
  assert.equal(maximum, 1);

  pool.resize(2);
  await Promise.resolve();
  assert.equal(maximum, 2);
  await pool.stop(context);
  assert.equal(active, 0);
  assert.equal(pool.executorsCount(), 2);
});

await test("task pool shrink lets admitted work finish before starting queued work", async () => {
  const context = Context.background();
  const pool = new TaskPool({ name: "shrink", executorsCount: 3 });
  const releases: (() => void)[] = [];
  let queuedStarted = false;
  await pool.start(context);

  for (let index = 0; index < 3; index += 1) {
    pool.addTask(
      context,
      () =>
        new Promise<void>((resolve) => {
          releases.push(resolve);
        })
    );
  }
  pool.addTask(context, () => {
    queuedStarted = true;
  });
  await Promise.resolve();
  assert.equal(pool.activeCount(), 3);

  pool.resize(1);
  releases[0]?.();
  await delay(1);
  assert.equal(pool.activeCount(), 2);
  assert.equal(queuedStarted, false);
  releases[1]?.();
  await delay(1);
  assert.equal(pool.activeCount(), 1);
  assert.equal(queuedStarted, false);
  releases[2]?.();
  await pool.stop(context);
  assert.equal(queuedStarted, true);
  assert.equal(pool.activeCount(), 0);
});

await test("async pools require at least one executor", () => {
  assert.throws(
    () => new TaskPool({ name: "invalid", executorsCount: 0 }),
    /executorsCount must be a positive integer/
  );
  assert.throws(
    () => new PriorityTaskPool({ name: "invalid-priority", executorsCount: 0 }),
    /executorsCount must be a positive integer/
  );
});

await test("cancelled queued work is promoted and executes with cancelled context", async () => {
  const context = Context.background();
  const controller = new AbortController();
  const cancelled = context.withExternalCancellation(controller.signal);
  const pool = new TaskPool({ name: "cancel", executorsCount: 1 });
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let observedCancellation = false;
  await pool.start(context);
  pool.addTask(context, () => gate);
  pool.addTask(cancelled, () => {
    observedCancellation = cancelled.cancelled();
  });
  controller.abort();
  release?.();
  await pool.stop(context);
  assert.equal(observedCancellation, true);
});

await test("task pool records the exact canonical metrics and lifecycle values", async () => {
  const metrics = new TestMetrics();
  const labels = { service: "Order Service", name: "default" };
  const context = Context.background();
  const pool = new TaskPool({
    name: "default",
    executorsCount: 2,
    metrics,
    service: "Order Service"
  });
  assert.deepEqual(metrics.registeredNames(), taskPoolMetricNames);
  assert.equal(metrics.gaugeValue("task_pool_executors_target", labels), 0);
  await pool.start(context);
  assert.equal(metrics.gaugeValue("task_pool_executors_target", labels), 2);
  assert.equal(metrics.gaugeValue("task_pool_executors_allocated", labels), 2);

  for (let index = 0; index < 3; index += 1) {
    pool.addTask(context, async () => {
      await delay(2);
    });
  }
  await Promise.resolve();
  assert.equal(metrics.gaugeValue("task_pool_executors_busy", labels), 2);
  assert.equal(metrics.gaugeValue("task_pool_queue_length", labels), 1);
  await pool.stop(context);

  assert.equal(metrics.gaugeValue("task_pool_executors_allocated", labels), 0);
  assert.equal(metrics.gaugeValue("task_pool_executors_busy", labels), 0);
  assert.equal(metrics.gaugeValue("task_pool_queue_length", labels), 0);
  assert.equal(metrics.counterValue("task_pool_tasks_total", labels), 3);
  assert.equal(
    metrics.histogramValue("task_pool_task_execution_duration_seconds", labels)?.count,
    3
  );
  assert.throws(() => {
    pool.addTask(context, () => undefined);
  }, PoolStoppedError);
  assert.equal(
    metrics.counterValue("task_pool_events_total", { ...labels, event: "task_rejected" }),
    1
  );
});

await test("priority pool records expiration, resize and exact canonical metrics", async () => {
  const metrics = new TestMetrics();
  const labels = { service: "Order Service", name: "priority" };
  const context = Context.background();
  const controller = new AbortController();
  const expired = context.withExternalCancellation(controller.signal);
  const pool = new PriorityTaskPool({
    name: "priority",
    executorsCount: 1,
    metrics,
    service: "Order Service"
  });
  assert.deepEqual(metrics.registeredNames(), priorityTaskPoolMetricNames);
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  await pool.start(context);
  pool.addTask(context, 0, () => gate);
  pool.addTask(context, 1, () => undefined);
  pool.addTask(expired, 100, () => undefined);
  await Promise.resolve();
  controller.abort(new Error("expired"));
  await Promise.resolve();
  pool.resize(2);
  assert.equal(metrics.gaugeValue("priority_task_pool_executors_target", labels), 2);
  assert.equal(metrics.gaugeValue("priority_task_pool_executors_allocated", labels), 2);
  release?.();
  await pool.stop(context);

  assert.equal(metrics.counterValue("priority_task_pool_tasks_total", labels), 3);
  assert.equal(
    metrics.counterValue("priority_task_pool_events_total", { ...labels, event: "task_expired" }),
    1
  );
  assert.equal(metrics.gaugeValue("priority_task_pool_queue_length", labels), 0);
  assert.equal(metrics.gaugeValue("priority_task_pool_executors_allocated", labels), 0);
});

await test("task pool stop deadline reports timeout but still drains admitted work", async () => {
  const metrics = new TestMetrics();
  const labels = { service: "Order Service", name: "timeout" };
  const pool = new TaskPool({
    name: "timeout",
    executorsCount: 1,
    metrics,
    service: "Order Service"
  });
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  await pool.start(Context.background());
  pool.addTask(Context.background(), () => gate);
  await Promise.resolve();
  const stopping = pool.stop(Context.background().bounded(2));
  await delay(10);
  assert.equal(
    metrics.counterValue("task_pool_events_total", { ...labels, event: "stop_timeout" }),
    1
  );
  release?.();
  await stopping;
  assert.equal(metrics.counterValue("task_pool_tasks_total", labels), 1);
});
