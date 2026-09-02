import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Context,
  errorFromUnknown,
  type AdmissionLifecycle,
  type Lifecycle,
  RuntimeStoppedError,
  RuntimeTaskRegistry,
  ServiceRuntime
} from "@gorundebug/tsservicelib/runtime";
import { TestLog } from "@gorundebug/tsservicelib/runtime/testlog";
import { makeTestEnvironment } from "./support/environment.js";

function lifecycle(name: string, events: string[], failStart = false): Lifecycle {
  return {
    start(): Promise<void> {
      events.push(`start:${name}`);
      if (failStart) {
        return Promise.reject(new Error(`failed:${name}`));
      }
      return Promise.resolve();
    },
    stop(): Promise<void> {
      events.push(`stop:${name}`);
      return Promise.resolve();
    }
  };
}

await test("service runtime follows canonical start ordering and rolls back partial start", async () => {
  const events: string[] = [];
  const runtime = new ServiceRuntime(makeTestEnvironment([]));
  runtime.register({ category: "dataSink", name: "sink", lifecycle: lifecycle("sink", events) });
  runtime.register({
    category: "dataSource",
    name: "source",
    lifecycle: lifecycle("source", events, true)
  });
  runtime.register({
    category: "storage",
    name: "storage",
    lifecycle: lifecycle("storage", events)
  });

  await assert.rejects(runtime.start(Context.background()), /failed:source/);
  assert.deepEqual(events, [
    "start:storage",
    "start:sink",
    "start:source",
    "stop:sink",
    "stop:storage"
  ]);
  assert.equal(runtime.state(), "stopped");
});

await test("service stop closes admission, drains accepted work, then stops sinks once", async () => {
  const events: string[] = [];
  const tasks = new RuntimeTaskRegistry();
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  runtime.register({
    category: "dataSource",
    name: "source",
    lifecycle: lifecycle("source", events)
  });
  runtime.register({ category: "dataSink", name: "sink", lifecycle: lifecycle("sink", events) });
  await runtime.start();
  const admitted = tasks.admit(async () => {
    await gate;
    events.push("task:done");
  });

  const stopping = runtime.stop(Context.background(), 100);
  await Promise.resolve();
  release?.();
  await admitted;
  await stopping;
  await runtime.stop();

  assert.deepEqual(events, ["start:sink", "start:source", "stop:source", "task:done", "stop:sink"]);
});

await test("graph pools stop before the accepted parallel-work counter drains", async () => {
  const events: string[] = [];
  const tasks = new RuntimeTaskRegistry();
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const sourceLifecycle: AdmissionLifecycle = {
    start(): Promise<void> {
      events.push("start:source");
      return Promise.resolve();
    },
    stopAdmission(): Promise<void> {
      events.push("admission:source");
      return Promise.resolve();
    },
    stop(): Promise<void> {
      events.push("stop:source");
      return Promise.resolve();
    }
  };
  runtime.register({
    category: "dataSource",
    name: "source",
    lifecycle: sourceLifecycle
  });
  runtime.register({
    category: "taskPool",
    name: "pool",
    lifecycle: lifecycle("pool", events)
  });
  await runtime.start();
  const admitted = tasks.admit(async () => {
    await gate;
    events.push("task:done");
  });

  const stopping = runtime.stop(Context.background(), 100);
  while (!events.includes("stop:pool")) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  release?.();
  await admitted;
  await stopping;

  assert.ok(events.indexOf("admission:source") < events.indexOf("stop:source"));
  assert.ok(events.indexOf("stop:source") < events.indexOf("stop:pool"));
  assert.ok(events.indexOf("stop:pool") < events.indexOf("task:done"));
});

await test("ordinary sources drain before managed connector admission stops", async () => {
  const events: string[] = [];
  const runtime = new ServiceRuntime(makeTestEnvironment([]));
  const source: AdmissionLifecycle = {
    start: () => Promise.resolve(),
    stopAdmission: () => {
      events.push("admission:source");
      return Promise.resolve();
    },
    stop: () => {
      events.push("stop:source");
      return Promise.resolve();
    }
  };
  const managedConnector: AdmissionLifecycle = {
    start: () => Promise.resolve(),
    stopAdmission: () => {
      events.push("admission:managed");
      return Promise.resolve();
    },
    stop: () => {
      events.push("stop:managed");
      return Promise.resolve();
    }
  };
  runtime.register({ category: "dataSource", name: "cron", lifecycle: source });
  runtime.register({
    category: "managedDataConnector",
    name: "temporal",
    lifecycle: managedConnector
  });

  await runtime.start();
  await runtime.stop();

  assert.deepEqual(events, [
    "admission:source",
    "stop:source",
    "admission:managed",
    "stop:managed"
  ]);
});

await test("accepted graph work may admit nested work while shutdown drains", async () => {
  const events: string[] = [];
  const errors: Error[] = [];
  const tasks = new RuntimeTaskRegistry((error) => errors.push(error));
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  let releaseParent: (() => void) | undefined;
  const parentGate = new Promise<void>((resolve) => {
    releaseParent = resolve;
  });

  runtime.register({
    category: "dataSource",
    name: "source",
    lifecycle: lifecycle("source", events)
  });
  runtime.register({ category: "dataSink", name: "sink", lifecycle: lifecycle("sink", events) });
  await runtime.start();
  const parent = tasks.admit(async () => {
    await parentGate;
    tasks.admitDetached(async () => {
      await Promise.resolve();
      events.push("nested:done");
    });
    events.push("parent:done");
  });

  const stopping = runtime.stop(Context.background(), 100);
  await Promise.resolve();
  releaseParent?.();
  await parent;
  await stopping;

  assert.deepEqual(errors, []);
  assert.deepEqual(events, [
    "start:sink",
    "start:source",
    "stop:source",
    "parent:done",
    "nested:done",
    "stop:sink"
  ]);
  await assert.rejects(
    tasks.admit(() => Promise.resolve()),
    RuntimeStoppedError
  );
});

await test("stopping a never-started runtime closes task admission", async () => {
  const tasks = new RuntimeTaskRegistry();
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  await runtime.stop();
  assert.equal(runtime.state(), "stopped");
  await assert.rejects(
    tasks.admit(() => Promise.resolve()),
    RuntimeStoppedError
  );
});

await test("service shutdown keeps sink and telemetry ownership phases deterministic", async () => {
  const events: string[] = [];
  const logs = new TestLog();
  const runtime = new ServiceRuntime(makeTestEnvironment([], { logger: logs.defaultLogger() }));
  runtime.register({ category: "dataSink", name: "sink", lifecycle: lifecycle("sink", events) });
  runtime.register({
    category: "storage",
    name: "storage",
    lifecycle: lifecycle("storage", events)
  });
  runtime.register({
    category: "telemetry",
    name: "logging",
    lifecycle: lifecycle("logging", events)
  });
  runtime.register({
    category: "telemetry",
    name: "tracing",
    lifecycle: lifecycle("tracing", events)
  });
  runtime.register({
    category: "telemetry",
    name: "metrics",
    lifecycle: {
      start(): Promise<void> {
        events.push("start:metrics");
        return Promise.resolve();
      },
      stop(): Promise<void> {
        events.push("stop:metrics");
        return Promise.reject(new Error("metrics shutdown failed"));
      }
    }
  });
  runtime.register({
    category: "telemetry",
    name: "diagnostics",
    lifecycle: lifecycle("diagnostics", events)
  });
  runtime.register({
    category: "dataSource",
    name: "source",
    lifecycle: lifecycle("source", events)
  });

  await runtime.start();
  events.length = 0;
  await runtime.stop();

  assert.deepEqual(events, [
    "stop:source",
    "stop:storage",
    "stop:sink",
    "stop:diagnostics",
    "stop:metrics",
    "stop:tracing",
    "stop:logging"
  ]);
  const warning = logs.entriesAtLevel("warn")[0];
  assert.ok(warning);
  assert.equal(warning.message, "runtime component shutdown");
  assert.deepEqual(warning.fields.slice(0, 2), [
    { key: "category", type: "string", value: "telemetry" },
    { key: "component", type: "string", value: "metrics" }
  ]);
});

await test("shutdown timeout is one upper bound shared by every stop phase", async () => {
  const events: string[] = [];
  const runtime = new ServiceRuntime(makeTestEnvironment([]));
  const slowLifecycle = (name: string): Lifecycle => ({
    start(): Promise<void> {
      return Promise.resolve();
    },
    async stop(): Promise<void> {
      events.push(`stop:${name}`);
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
    }
  });
  runtime.register({ category: "dataSource", name: "source", lifecycle: slowLifecycle("source") });
  runtime.register({ category: "dataSink", name: "sink", lifecycle: slowLifecycle("sink") });
  runtime.register({
    category: "telemetry",
    name: "telemetry",
    lifecycle: slowLifecycle("telemetry")
  });

  await runtime.start();
  const started = performance.now();
  await runtime.stop(Context.background(), 20);
  const elapsed = performance.now() - started;

  assert.ok(elapsed < 80, `shutdown restarted its deadline between phases: ${String(elapsed)}ms`);
  assert.deepEqual(events, ["stop:source", "stop:sink", "stop:telemetry"]);
  await new Promise<void>((resolve) => setTimeout(resolve, 105));
});

await test("stop during startup cancels admission and rolls back every started component", async () => {
  const events: string[] = [];
  const tasks = new RuntimeTaskRegistry();
  const runtime = new ServiceRuntime(makeTestEnvironment([]), tasks);
  let enteredSecond: (() => void) | undefined;
  const secondEntered = new Promise<void>((resolve) => {
    enteredSecond = resolve;
  });
  runtime.register({
    category: "dataSource",
    name: "first",
    lifecycle: lifecycle("first", events)
  });
  runtime.register({
    category: "dataSource",
    name: "second",
    lifecycle: {
      async start(context): Promise<void> {
        events.push("start:second");
        enteredSecond?.();
        await new Promise<void>((_resolve, reject) => {
          const cancelled = (): void => {
            reject(errorFromUnknown(context.signal().reason));
          };
          if (context.cancelled()) cancelled();
          else context.signal().addEventListener("abort", cancelled, { once: true });
        });
      },
      stop(): Promise<void> {
        events.push("stop:second");
        return Promise.resolve();
      }
    }
  });
  runtime.register({
    category: "dataSource",
    name: "must-not-start",
    lifecycle: lifecycle("must-not-start", events)
  });

  const starting = runtime.start();
  await secondEntered;
  const stopping = runtime.stop(Context.background().bounded(100), 100);

  await assert.rejects(starting, RuntimeStoppedError);
  await stopping;
  assert.equal(runtime.state(), "stopped");
  assert.equal(tasks.accepting(), false);
  assert.deepEqual(events, ["start:first", "start:second", "stop:first"]);
});
