import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Context,
  MessageContext,
  ParallelCaller,
  PriorityTaskPool,
  PriorityTaskPoolCaller,
  RuntimeTaskRegistry,
  TaskPool,
  TaskPoolCaller,
  type Consumer
} from "@gorundebug/tsservicelib/runtime";

function numberConsumer(values: number[]): Consumer<number> {
  return {
    consume(_context, value): void {
      values.push(value);
    }
  };
}

await test("task pool caller schedules delivery and reports rejection without throwing", async () => {
  const context = new MessageContext();
  const pool = new TaskPool({ name: "tasks", executorsCount: 1 });
  const values: number[] = [];
  const errors: unknown[] = [];
  const caller = new TaskPoolCaller(pool, numberConsumer(values), (error) => {
    errors.push(error);
  });
  await pool.start(context);
  caller.consume(context, 1);
  await pool.stop(context);
  caller.consume(context, 2);

  assert.equal(caller.isAsync(), true);
  assert.deepEqual(values, [1]);
  assert.equal(errors.length, 1);
});

await test("priority caller preserves explicit context priority zero", async () => {
  const context = new MessageContext();
  const pool = new PriorityTaskPool({ name: "priority", executorsCount: 1 });
  const values: number[] = [];
  const caller = new PriorityTaskPoolCaller(pool, numberConsumer(values), 10);
  caller.consume(context, 10);
  caller.consume(context.withPriority(0), 0);
  await pool.start(context);
  await pool.stop(context);

  assert.deepEqual(values, [0, 10]);
});

await test("parallel caller registers detached work for deterministic drain", async () => {
  const registry = new RuntimeTaskRegistry();
  const values: number[] = [];
  const caller = new ParallelCaller(registry, numberConsumer(values));

  caller.consume(new MessageContext(), 1);
  assert.equal(registry.activeCount(), 1);
  registry.stopAdmission();
  await registry.drain();

  assert.deepEqual(values, [1]);
  assert.equal(registry.activeCount(), 0);
  assert.equal(Context.background().cancelled(), false);
});
