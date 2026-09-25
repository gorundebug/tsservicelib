import assert from "node:assert/strict";
import { test } from "node:test";

import { Context, RuntimeTaskRegistry, ServiceRuntime } from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

function barrier(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function bounded<T>(operation: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error("runtime stop exceeded its context"));
        }, 2_000);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

for (const reason of ["cancelled", "expired"] as const) {
  await test(`empty runtime task registry stops with an already ${reason} context`, async () => {
    const tasks = new RuntimeTaskRegistry();
    const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
    await runtime.start();
    const context =
      reason === "cancelled"
        ? new Context(AbortSignal.abort(new Error("stop requested")))
        : Context.background().bounded(0);
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    await assert.doesNotReject(runtime.stop(context));
    assert.equal(runtime.state(), "stopped");
    assert.equal(tasks.activeCount(), 0);
    assert.equal(tasks.accepting(), false);
  });
}

await test("runtime task drain observes stop cancellation without a deadline", async () => {
  const tasks = new RuntimeTaskRegistry();
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  const gate = barrier();
  await runtime.start();
  const active = tasks.admit(async () => {
    await gate.promise;
  });
  const cancellation = new AbortController();
  const reason = new Error("stop requested");
  const stopping = runtime.stop(new Context(cancellation.signal));
  const outcome = stopping.then(
    () => undefined,
    (error: unknown) => error
  );
  try {
    cancellation.abort(reason);
    assert.equal(await bounded(outcome), reason);
    assert.equal(runtime.state(), "stopped");
    assert.equal(tasks.accepting(), false);
  } finally {
    gate.resolve();
    await active;
    await outcome;
  }
});

await test("runtime stop bounds uncooperative startup and keeps rollback owned by startup", async () => {
  const tasks = new RuntimeTaskRegistry();
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  const entered = barrier();
  const release = barrier();
  const events: string[] = [];
  runtime.register({
    category: "component",
    name: "dependency",
    lifecycle: {
      start: () => {
        events.push("dependency:start");
        return Promise.resolve();
      },
      stop: () => {
        events.push("dependency:stop");
        return Promise.resolve();
      }
    }
  });
  runtime.register({
    category: "component",
    name: "blocked-start",
    lifecycle: {
      async start() {
        entered.resolve();
        await release.promise;
        events.push("blocked:start-returned");
      },
      stop: () => {
        events.push("blocked:stop");
        return Promise.resolve();
      }
    }
  });
  const starting = runtime.start().then(
    () => undefined,
    (error: unknown) => error
  );
  let stopping: Promise<void> | undefined;
  try {
    await bounded(entered.promise);
    stopping = runtime.stop(Context.background().bounded(20));
    await bounded(stopping);
    assert.equal(runtime.state(), "stopped");
    assert.equal(tasks.accepting(), false);
    assert.deepEqual(events, ["dependency:start"], "startup may still be using its dependency");
  } finally {
    release.resolve();
    assert.ok((await starting) instanceof Error);
    await stopping;
  }
  assert.deepEqual(events, [
    "dependency:start",
    "blocked:start-returned",
    "blocked:stop",
    "dependency:stop"
  ]);
});
