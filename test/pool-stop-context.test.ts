import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { test } from "node:test";
import {
  Context,
  DelayPool,
  PoolStoppedError,
  PriorityTaskPool,
  RuntimeDrainTimeoutError,
  ServiceRuntime,
  TaskPool
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

type Kind = "fifo" | "priority" | "delay";
type Pool = TaskPool | PriorityTaskPool | DelayPool;
function makePool(kind: Kind): Pool {
  return kind === "delay"
    ? new DelayPool()
    : kind === "fifo"
      ? new TaskPool({ name: kind, executorsCount: 1 })
      : new PriorityTaskPool({ name: kind, executorsCount: 1 });
}
function add(pool: Pool, context: Context, work: () => void | Promise<void>): void {
  if (pool instanceof DelayPool) pool.delay(context, 0, work);
  else if (pool instanceof PriorityTaskPool) pool.addTask(context, 0, work);
  else pool.addTask(context, work);
}
function active(pool: Pool): number {
  return pool instanceof DelayPool ? pool.pendingCount() : pool.activeCount();
}
function gate(): { promise: Promise<void>; resolve: () => void } {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
for (const kind of ["fifo", "priority", "delay"] as const) {
  await test(
    kind + ": each stop observes its own context without dropping physical work",
    async () => {
      const pool = makePool(kind);
      const owner = Context.background();
      const held = gate();
      const entered = gate();
      let completed = 0;
      await pool.start(owner);
      add(pool, owner, async () => {
        entered.resolve();
        await held.promise;
        completed++;
      });
      await entered.promise;
      try {
        await assert.rejects(pool.stop(owner.bounded(5)), RuntimeDrainTimeoutError);
        assert.equal(active(pool), 1);
        assert.equal(owner.cancelled(), false);
        assert.throws(() => {
          add(pool, owner, () => undefined);
        }, PoolStoppedError);
        const cancellation = new AbortController();
        const reason = new Error("retry caller cancelled");
        const retryContext = new Context(cancellation.signal);
        const waiting = pool.stop(retryContext);
        const rejected = assert.rejects(waiting, (error: unknown) => error === reason);
        cancellation.abort(reason);
        await rejected;
        assert.equal(getEventListeners(retryContext.signal(), "abort").length, 0);
        assert.equal(active(pool), 1);
        await assert.rejects(pool.stop(owner.bounded(5)), RuntimeDrainTimeoutError);
        const final = pool.stop(owner);
        await Promise.resolve();
        assert.equal(completed, 0);
        held.resolve();
        await final;
        assert.equal(completed, 1);
        assert.equal(active(pool), 0);
        await pool.stop(new Context(AbortSignal.abort(reason)));
      } finally {
        held.resolve();
        await pool.stop(owner);
      }
    }
  );

  await test(kind + ": service retains downstream sinks until the actual pool drains", async () => {
    const pool = makePool(kind);
    const runtime = new ServiceRuntime(makeTestEnvironment([]));
    const held = gate();
    const entered = gate();
    const events: string[] = [];
    runtime.register({
      category: kind === "delay" ? "delayPool" : kind === "fifo" ? "taskPool" : "priorityTaskPool",
      name: kind,
      lifecycle: pool
    });
    runtime.register({
      category: "dataSink",
      name: "sink",
      lifecycle: {
        start: () => Promise.resolve(),
        stop: () => {
          events.push("sink:stop");
          return Promise.resolve();
        }
      }
    });
    const owner = Context.background();
    await runtime.start(owner);
    add(pool, owner, async () => {
      entered.resolve();
      await held.promise;
      events.push("task:done");
    });
    await entered.promise;
    try {
      await assert.rejects(runtime.stop(owner.bounded(5)), RuntimeDrainTimeoutError);
      assert.equal(runtime.state(), "stopping");
      assert.deepEqual(events, []);
      assert.equal(active(pool), 1);
      const retry = runtime.stop(owner);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.deepEqual(events, []);
      held.resolve();
      await retry;
      assert.deepEqual(events, ["task:done", "sink:stop"]);
      assert.equal(runtime.state(), "stopped");
    } finally {
      held.resolve();
      await runtime.stop(owner);
    }
  });

  await test(kind + ": empty pool stop succeeds even with an expired caller", async () => {
    const pool = makePool(kind);
    await pool.start(Context.background());
    await pool.stop(new Context(AbortSignal.abort(new Error("already cancelled"))));
    assert.equal(active(pool), 0);
  });
}

await test("pool drain rechecks a deadline after an early timer wakeup", async () => {
  class ControlledDeadline extends Context {
    public remaining = 5;
    public override remainingMs(): number {
      return this.remaining;
    }
  }
  const context = new ControlledDeadline(new AbortController().signal);
  const pool = new TaskPool({ name: "early-timer", executorsCount: 1 });
  const held = gate();
  await pool.start(Context.background());
  pool.addTask(Context.background(), () => held.promise);
  let settled = false;
  const outcome = pool.stop(context).then(
    () => {
      settled = true;
      return undefined;
    },
    (error: unknown) => {
      settled = true;
      return error;
    }
  );
  try {
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
    assert.equal(settled, false, "early timer must not finish a wait before its deadline");
    context.remaining = 0;
    assert.ok((await outcome) instanceof RuntimeDrainTimeoutError);
    assert.equal(pool.activeCount(), 1);
  } finally {
    held.resolve();
    await outcome;
    await pool.stop(Context.background());
  }
});

for (const cancellationFailure of [true, false]) {
  await test(
    "runtime joins an old component stop without hiding genuine failures: " +
      String(cancellationFailure),
    async () => {
      const runtime = new ServiceRuntime(makeTestEnvironment([]));
      const oldEntered = gate();
      const freshEntered = gate();
      const physicalDrain = gate();
      let failOld: (error: Error) => void = () => undefined;
      const oldWait = new Promise<void>((_resolve, reject) => {
        failOld = reject;
      });
      const contexts: Context[] = [];
      let sinkStopped = false;
      runtime.register({
        category: "taskPool",
        name: "retry-context",
        lifecycle: {
          start: () => Promise.resolve(),
          stop(context) {
            contexts.push(context);
            if (contexts.length === 1) {
              oldEntered.resolve();
              return oldWait;
            }
            freshEntered.resolve();
            return physicalDrain.promise;
          }
        }
      });
      runtime.register({
        category: "dataSink",
        name: "sink",
        lifecycle: {
          start: () => Promise.resolve(),
          stop: () => {
            sinkStopped = true;
            return Promise.resolve();
          }
        }
      });
      await runtime.start(Context.background());
      const expired = Context.background().bounded(5);
      const current = Context.background();
      const oldError = cancellationFailure
        ? new RuntimeDrainTimeoutError(5)
        : new Error("native stop failed");
      try {
        await assert.rejects(runtime.stop(expired), RuntimeDrainTimeoutError);
        await oldEntered.promise;
        const retryOutcome = runtime.stop(current).then(
          () => undefined,
          (error: unknown) => error
        );
        // Allow the new stop to join the still-pending old component wait.
        await new Promise<void>((resolve) => setImmediate(resolve));
        failOld(oldError);
        if (cancellationFailure) {
          await freshEntered.promise;
          assert.deepEqual(contexts, [expired, current]);
          assert.equal(sinkStopped, false);
          physicalDrain.resolve();
          assert.equal(await retryOutcome, undefined);
          assert.equal(runtime.state(), "stopped");
          assert.equal(sinkStopped, true);
        } else {
          assert.equal(
            await retryOutcome,
            oldError,
            "real component errors must not become implicit retries"
          );
          assert.equal(contexts.length, 1);
          assert.equal(sinkStopped, false);
          assert.equal(runtime.state(), "stopping");
        }
      } finally {
        failOld(oldError);
        physicalDrain.resolve();
        await runtime.stop(Context.background());
      }
    }
  );
}
