import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { Context, MessageContext } from "@gorundebug/tsservicelib/runtime/graph";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

import type * as WorkflowPools from "../src/datasource/temporal/workflow-pool.js";

const { WorkflowTaskPool, WorkflowPriorityTaskPool } = (await import(
  pathToFileURL(resolve("dist/datasource/temporal/workflow-pool.js")).href
)) as typeof WorkflowPools;

for (const Pool of [WorkflowTaskPool, WorkflowPriorityTaskPool]) {
  for (const cancelled of [false, true]) {
    await test(`${Pool.name} reports the task context, cancellation=${String(cancelled)}`, async () => {
      const errors: { error: unknown; context: Context }[] = [];
      const pool = new Pool("workflow", 1, new TestMetrics(), "test", (error, context) => {
        errors.push({ error, context });
      });
      const controller = new AbortController();
      const context = new MessageContext(controller.signal).withStreamId("workflow-task");
      const failure = new Error("workflow task failed");
      const execute = (): void => {
        if (cancelled) controller.abort(failure);
        throw failure;
      };
      if (pool instanceof WorkflowPriorityTaskPool) pool.addTask(context, 0, execute);
      else pool.addTask(context, execute);
      pool.start();
      await pool.stop();
      assert.deepEqual(errors, [{ error: failure, context }]);
      assert.equal(pool.activeCount(), 0);
      assert.equal(context.cancelled(), cancelled);
    });
  }

  await test(`${Pool.name} preserves ordering, cancellation and shared drain`, async () => {
    const pool = new Pool("workflow", 0, new TestMetrics(), "test", () => {
      throw new Error("reporter failed");
    });
    // A workflow's default must be replay-stable, regardless of the host CPUs.
    assert.equal(pool.executorsCount(), 1);
    const context = Context.background();
    const controller = new AbortController();
    const cancelled = context.withExternalCancellation(controller.signal);
    const seen: number[] = [];
    const add = (ctx: Context, n: number): void => {
      const callback = (): void => {
        seen.push(n);
      };
      if (pool instanceof WorkflowPriorityTaskPool) pool.addTask(ctx, n, callback);
      else pool.addTask(ctx, callback);
    };
    add(context, 3);
    add(context, 1);
    add(cancelled, 9);
    controller.abort();
    await pool.stop();
    assert.deepEqual(seen, pool instanceof WorkflowPriorityTaskPool ? [9, 1, 3] : [9, 3, 1]);
    assert.equal(pool.queueLength(), 0);
  });

  await test(`${Pool.name} keeps callbacks independent and drains despite reporter failure`, async () => {
    const pool = new Pool("workflow", 2, new TestMetrics(), "test", () => {
      throw new Error("reporter failed");
    });
    let release = (): void => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const add = (callback: () => Promise<void>): void => {
      if (pool instanceof WorkflowPriorityTaskPool) pool.addTask(Context.background(), 0, callback);
      else pool.addTask(Context.background(), callback);
    };
    let progressed = false;
    add(() => gate);
    add(async () => {
      await delay(1);
      progressed = true;
      throw new Error("callback failed");
    });
    pool.start();
    let stopped = 0;
    const a = pool.stop().then(() => {
      stopped++;
    });
    const b = pool.stop().then(() => {
      stopped++;
    });
    await delay(10);
    assert.equal(progressed, true);
    assert.equal(stopped, 0);
    release();
    await Promise.all([a, b]);
    assert.equal(stopped, 2);
  });
}
