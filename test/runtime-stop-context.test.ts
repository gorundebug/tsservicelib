import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Context,
  RuntimeDrainTimeoutError,
  RuntimeTaskRegistry,
  ServiceRuntime
} from "@gorundebug/tsservicelib/runtime";

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
    await runtime.start(Context.background());
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
  await runtime.start(Context.background());
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
    assert.equal(runtime.state(), "stopping");
    assert.equal(tasks.accepting(), false);
  } finally {
    gate.resolve();
    await active;
    await outcome;
    await runtime.stop(Context.background());
    assert.equal(runtime.state(), "stopped");
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
  const starting = runtime.start(Context.background()).then(
    () => undefined,
    (error: unknown) => error
  );
  let stopping: Promise<void> | undefined;
  try {
    await bounded(entered.promise);
    stopping = runtime.stop(Context.background().bounded(20));
    await assert.rejects(bounded(stopping), RuntimeDrainTimeoutError);
    assert.equal(runtime.state(), "stopping");
    assert.equal(tasks.accepting(), false);
    assert.deepEqual(events, ["dependency:start"], "startup may still be using its dependency");
  } finally {
    release.resolve();
    assert.ok((await starting) instanceof Error);
    await stopping?.catch(() => undefined);
    await runtime.stop(Context.background());
    assert.equal(runtime.state(), "stopped");
  }
  assert.deepEqual(events, [
    "dependency:start",
    "blocked:start-returned",
    "blocked:stop",
    "dependency:stop"
  ]);
});

await test("runtime retains pending component stops and dependencies across retry", async () => {
  const runtime = new ServiceRuntime(makeTestEnvironment([]));
  const entered = barrier();
  const release = barrier();
  const events: string[] = [];
  let calls = 0;
  runtime.register({
    category: "dataSource",
    name: "held",
    lifecycle: {
      start: () => Promise.resolve(),
      async stop() {
        calls++;
        entered.resolve();
        await release.promise;
        events.push("source:done");
      }
    }
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
  await runtime.start(Context.background());
  try {
    await assert.rejects(runtime.stop(Context.background(), 10), RuntimeDrainTimeoutError);
    assert.equal(runtime.state(), "stopping");
    assert.deepEqual(events, []);
    const retry = runtime.stop(Context.background().bounded(1_000));
    await entered.promise;
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(calls, 1, "unfinished stop must not be invoked again");
    assert.deepEqual(events, []);
    release.resolve();
    await bounded(retry);
    assert.equal(runtime.state(), "stopped");
    assert.deepEqual(events, ["source:done", "sink:stop"]);
  } finally {
    release.resolve();
    await runtime.stop(Context.background());
  }
});

await test("concurrent stop callers retain independent cancellation and retry ownership", async () => {
  const runtime = new ServiceRuntime(makeTestEnvironment([]));
  const entered = barrier();
  const release = barrier();
  const owner = new AbortController();
  const firstContext = new Context(owner.signal);
  const reason = new Error("first stop cancelled");
  let calls = 0;
  runtime.register({
    category: "dataSource",
    name: "held",
    lifecycle: {
      start: () => Promise.resolve(),
      async stop(context) {
        assert.equal(context, firstContext);
        calls++;
        entered.resolve();
        await release.promise;
      }
    }
  });
  await runtime.start(Context.background());
  const first = runtime.stop(firstContext);
  const firstOutcome = first.then(
    () => undefined,
    (error: unknown) => error
  );
  await entered.promise;
  const second = runtime.stop(Context.background().bounded(1_000));
  let secondFinished = false;
  void second.then(
    () => {
      secondFinished = true;
    },
    () => {
      secondFinished = true;
    }
  );
  try {
    owner.abort(reason);
    assert.equal(await bounded(firstOutcome), reason);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(secondFinished, false);
    assert.equal(calls, 1);
    release.resolve();
    await bounded(second);
    assert.equal(runtime.state(), "stopped");
  } finally {
    release.resolve();
    await firstOutcome;
    await second;
  }
});

await test("failed component stop is retried with the new caller context", async () => {
  const runtime = new ServiceRuntime(makeTestEnvironment([]));
  const failure = new Error("temporary stop failure");
  const contexts: Context[] = [];
  const first = Context.background();
  const second = Context.background().bounded(1_000);
  runtime.register({
    category: "dataSink",
    name: "retryable",
    lifecycle: {
      start: () => Promise.resolve(),
      stop(context) {
        contexts.push(context);
        return contexts.length === 1 ? Promise.reject(failure) : Promise.resolve();
      }
    }
  });
  await runtime.start(Context.background());
  await assert.rejects(runtime.stop(first), (error: unknown) => error === failure);
  assert.equal(
    runtime.tasks().accepting(),
    false,
    "failed stop must seal task admission before rejecting"
  );
  assert.equal(runtime.state(), "stopping");
  await runtime.stop(second);
  assert.deepEqual(contexts, [first, second]);
  assert.equal(runtime.state(), "stopped");
});
