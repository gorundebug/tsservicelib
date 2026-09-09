import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { Context } from "@gorundebug/tsservicelib/runtime/graph";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type * as WorkflowPools from "../src/datasource/temporal/workflow-pool.js";

const { WorkflowTaskPool, WorkflowPriorityTaskPool } = (await import(
  pathToFileURL(resolve("dist/datasource/temporal/workflow-pool.js")).href
)) as typeof WorkflowPools;

for (const Pool of [WorkflowTaskPool, WorkflowPriorityTaskPool]) {
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
