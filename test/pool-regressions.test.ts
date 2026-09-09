import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { getEventListeners } from "node:events";
import { availableParallelism } from "node:os";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { Context, DelayPool, TaskPool, PriorityTaskPool } from "@gorundebug/tsservicelib/runtime";

type Callback = Parameters<TaskPool["addTask"]>[1];
function makeGate(): { promise: Promise<void>; resolve: () => void } {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const context = Context.background();
for (const kind of ["fifo", "priority", "delay"] as const) {
  await test(`${kind}: independent callbacks, context isolation and concurrent shutdown`, async () => {
    const onError = (): void => {
      throw new Error("reporter failed");
    };
    const pool =
      kind === "delay"
        ? new DelayPool({ onError })
        : kind === "fifo"
          ? new TaskPool({ name: kind, executorsCount: 2, onError })
          : new PriorityTaskPool({ name: kind, executorsCount: 2, onError });
    const add = (fn: Callback): void => {
      if (pool instanceof DelayPool) pool.delay(context, 0, fn);
      else if (pool instanceof PriorityTaskPool) pool.addTask(context, 0, fn);
      else pool.addTask(context, fn);
    };
    const local = new AsyncLocalStorage<string>();
    const gate = makeGate();
    const seen: string[] = [];
    local.run("first", () => {
      add(async () => {
        seen.push(local.getStore() ?? "missing");
        await gate.promise;
        assert.equal(local.getStore(), "first");
      });
    });
    local.run("second", () => {
      add(async () => {
        await delay(1);
        seen.push(local.getStore() ?? "missing");
        throw new Error("callback failed");
      });
    });
    // Queue before start; later callbacks must retain their own admission context.
    local.run("third", () => {
      add(() => {
        seen.push(local.getStore() ?? "missing");
      });
    });
    await pool.start(context);
    let stopped = 0;
    const first = pool.stop(context.bounded(1)).then(() => {
      stopped++;
    });
    const second = pool.stop(context).then(() => {
      stopped++;
    });
    await delay(20);
    assert.deepEqual([...seen].sort(), ["first", "second", "third"]);
    assert.equal(stopped, 0);
    gate.resolve();
    await Promise.all([first, second]);
    assert.equal(stopped, 2);
  });
}

for (const Pool of [TaskPool, PriorityTaskPool]) {
  await test(`${Pool.name}: zero uses CPU count and stop drains prestart callbacks`, async () => {
    const pool = new Pool({ name: "prestart", executorsCount: 0 });
    assert.equal(pool.executorsCount(), availableParallelism());
    let completed = 0;
    const fn = (): void => {
      completed++;
    };
    if (pool instanceof PriorityTaskPool) pool.addTask(context, 0, fn);
    else pool.addTask(context, fn);
    await pool.stop(context);
    assert.equal(completed, 1);
  });
  await test(`${Pool.name}: cancellation promotes without duplication or listener fanout`, async () => {
    const pool = new Pool({ name: "cancel", executorsCount: 1 });
    const controller = new AbortController();
    const cancelled = context.withExternalCancellation(controller.signal);
    const order: number[] = [];
    const add = (ctx: Context, n: number): void => {
      const fn = (): void => {
        order.push(n);
      };
      if (pool instanceof PriorityTaskPool) pool.addTask(ctx, n, fn);
      else pool.addTask(ctx, fn);
    };
    add(context, -1);
    for (let n = 0; n < 200; n++) add(cancelled, n);
    assert.equal(getEventListeners(cancelled.signal(), "abort").length, 1);
    controller.abort();
    await pool.stop(context);
    assert.equal(order.length, 201);
    assert.equal(new Set(order).size, 201);
    assert.equal(order.at(-1), -1);
    assert.equal(getEventListeners(cancelled.signal(), "abort").length, 0);
  });
}

await test("delay: abort is asynchronous, large delays never overflow and all callbacks drain", async () => {
  const pool = new DelayPool();
  const controller = new AbortController();
  const cancelled = context.withExternalCancellation(controller.signal);
  let calls = 0;
  for (let i = 0; i < 200; i++)
    pool.delay(cancelled, 3_000_000_000, () => {
      calls++;
    });
  assert.equal(getEventListeners(cancelled.signal(), "abort").length, 1);
  await delay(10);
  assert.equal(calls, 0);
  controller.abort();
  assert.equal(calls, 0);
  await pool.stop(context);
  assert.equal(calls, 200);
  assert.equal(pool.pendingCount(), 0);
  assert.equal(getEventListeners(cancelled.signal(), "abort").length, 0);
});

await test("priority: shrinking retains busy tasks and starts queued work at the new bound", async () => {
  const pool = new PriorityTaskPool({ name: "shrink", executorsCount: 3 });
  const gates = Array.from({ length: 3 }, makeGate);
  await pool.start(context);
  for (const gate of gates) pool.addTask(context, 0, () => gate.promise);
  let ran = false;
  pool.addTask(context, 1, () => {
    ran = true;
  });
  await delay(1);
  pool.resize(1);
  gates[0]?.resolve();
  gates[1]?.resolve();
  await delay(5);
  assert.equal(ran, false);
  assert.equal(pool.activeCount(), 1);
  gates[2]?.resolve();
  await pool.stop(context);
  assert.equal(ran, true);
});
