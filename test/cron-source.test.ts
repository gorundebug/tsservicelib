import assert from "node:assert/strict";
import { test } from "node:test";

import { Cron } from "croner";

import { makeCronEndpointConsumer, parseLocalScheduleInterval } from "@gorundebug/tsservicelib/datasource/cron";
import { InputStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  MessageContext,
  MessageContextKey,
  type Collector,
  DataConnectorType,
  ScheduleBackend,
  ServiceStream,
  type CronEndpointConfig,
  type InputStreamConfig,
  type ScheduleEndpointFunction,
  type ScheduleTrigger
} from "@gorundebug/tsservicelib/runtime";

import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

const streamConfig: InputStreamConfig = {
  id: 1,
  name: "everySecond",
  properties: {},
  type: "Input",
  pipeline: "scheduled",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  idEndpoint: 100,
  valueType: "ScheduleTrigger"
};

const endpoint: CronEndpointConfig = {
  id: 100,
  name: "every-second",
  properties: {},
  idDataConnector: 10,
  enabled: true,
  schedule: "* * * * * *",
  timezone: "UTC",
  overlapPolicy: "Skip",
  missedRunPolicy: "FireOnce"
};

const resultConfig = {
  ...streamConfig,
  id: 3,
  name: "scheduledResult",
  type: "Map" as const,
  idEndpoint: 0
};

class TriggerConsumer extends ServiceStream {
  public readonly received = Promise.withResolvers<string>();

  public consume(_context: MessageContext, value: string): void {
    this.received.resolve(value);
  }
}

class ResultAwareTriggerConsumer extends ServiceStream {
  public readonly received = Promise.withResolvers<MessageContext>();

  public consume(context: MessageContext): void {
    this.received.resolve(context);
  }
}

const function_: ScheduleEndpointFunction<string> = {
  onTrigger(context: MessageContext, trigger: Readonly<ScheduleTrigger>, out: Collector<string>) {
    return out.out(context, `${trigger.scheduleId}:${trigger.backend}`);
  }
};

await test("local interval parser keeps canonical millisecond bounds", () => {
  for (const value of [10, 100, 1_000, 86_400_000]) {
    assert.equal(parseLocalScheduleInterval(`@every ${String(value)}ms`), value);
  }
  assert.equal(parseLocalScheduleInterval("* * * * *"), undefined);
  for (const value of ["@every", "@every 9ms", "@every 86400001ms", "@every 010ms",
    "@every +10ms", "@every -10ms", "@every 1s", "@every 0.1s", "@every  100ms",
    "@every 100ms ", "@every 100ms\n"]) {
    assert.throws(() => parseLocalScheduleInterval(value), /local interval requires/);
  }
});

await test("native interval keeps cadence, owner metadata and shutdown", { timeout: 5_000 }, async () => {
  const entered = Promise.withResolvers<undefined>();
  const values: Readonly<ScheduleTrigger>[] = [];
  const cancellation = new AbortController();
  const owner = new MessageContext(cancellation.signal).withMetadata(new Map([["owner", "interval"]]));
  const { source } = ownerContextSource((context, trigger) => {
    assert.equal(context.metadata().get("owner"), "interval");
    assert.ok(context.streamId());
    values.push(trigger);
    if (values.length === 3) entered.resolve(undefined);
  }, { ...endpoint, schedule: "@every 100ms" });
  try {
    await source.start(owner);
    await entered.promise;
    cancellation.abort();
    await source.stop(new Context().bounded(1_000));
    assert.ok(values.length >= 3);
    for (let i = 1; i < values.length; i++) {
      const current = values[i];
      const previous = values[i - 1];
      assert.ok(current && previous);
      const delta = Date.parse(current.scheduledAt) - Date.parse(previous.scheduledAt);
      assert.ok(delta >= 100 && delta % 100 === 0);
      assert.notEqual(current.triggerId, previous.triggerId);
    }
    const count = values.length;
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(values.length, count);
  } finally {
    cancellation.abort();
    await source.stop(new Context().bounded(1_000));
  }
});

await test("native interval skips overlap and retains accepted work during stop", { timeout: 5_000 }, async () => {
  const entered = Promise.withResolvers<MessageContext>();
  const release = Promise.withResolvers<undefined>();
  let calls = 0;
  const { source } = ownerContextSource(async (context) => {
    calls++;
    entered.resolve(context);
    await release.promise;
  }, { ...endpoint, schedule: "@every 100ms" });
  try {
    await source.start(new Context());
    await entered.promise;
    await new Promise((resolve) => setTimeout(resolve, 250));
    assert.equal(calls, 1);
    await assert.rejects(source.stop(new Context().bounded(5)), { name: "RuntimeDrainTimeoutError" });
    await assert.rejects(source.start(new Context()), /already started/);
  } finally {
    release.resolve(undefined);
    await source.stop(new Context().bounded(1_000));
  }
});

await test("Croner datasource invokes the endpoint function before the input stream", async () => {
  const consumerConfig = {
    ...streamConfig,
    id: 2,
    name: "consumeTrigger",
    type: "Map" as const,
    idSource: streamConfig.id
  };
  const environment = makeTestEnvironment([streamConfig, consumerConfig], {
    dataConnectors: [
      {
        id: 10,
        name: "localCron",
        properties: {},
        type: DataConnectorType.Cron,
        implementation: "node/croner"
      }
    ],
    endpoints: [endpoint]
  });
  const input = new InputStream<string, never, Error>(
    streamConfig,
    environment,
    makeTestSerde(),
    makeTestSerde()
  );
  const consumer = new TriggerConsumer(consumerConfig, environment);
  input.setConsumer(consumer);
  makeCronEndpointConsumer(input, function_);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);

  await dataSource.start(Context.background());
  const value = await Promise.race([
    consumer.received.promise,
    new Promise<never>((_resolve, reject) => {
      setTimeout(() => {
        reject(new Error("cron trigger timeout"));
      }, 2_500).unref();
    })
  ]);
  await dataSource.stop(Context.background());

  assert.equal(value, `${endpoint.name}:${ScheduleBackend.Local}`);
});

await test("Croner evaluates the portable schedule in UTC", () => {
  const job = new Cron("30 2 * * *", {
    paused: true,
    timezone: "UTC"
  });
  const next = job.nextRun(new Date("2026-03-07T03:00:00Z"));
  assert.ok(next);
  assert.equal(next.toISOString(), "2026-03-08T02:30:00.000Z");
});

await test("Croner source waits for its correlated pipeline result during stop", async () => {
  const consumerConfig = {
    ...streamConfig,
    id: 2,
    name: "consumeTriggerWithResult",
    type: "Map" as const,
    idSource: streamConfig.id
  };
  const environment = makeTestEnvironment([streamConfig, consumerConfig, resultConfig], {
    dataConnectors: [
      {
        id: 10,
        name: "localCron",
        properties: {},
        type: DataConnectorType.Cron,
        implementation: "node/croner"
      }
    ],
    endpoints: [endpoint]
  });
  const input = new InputStream<string, string, Error>(
    streamConfig,
    environment,
    makeTestSerde(),
    makeTestSerde()
  );
  const result = new ConsumedStream<string>(resultConfig, environment, makeTestSerde());
  input.setSource(result);
  const consumer = new ResultAwareTriggerConsumer(consumerConfig, environment);
  input.setConsumer(consumer);
  makeCronEndpointConsumer(input, function_);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);

  await dataSource.start(Context.background());
  const resultContext = await Promise.race([
    consumer.received.promise,
    new Promise<never>((_resolve, reject) => {
      setTimeout(() => {
        reject(new Error("cron trigger timeout"));
      }, 2_500).unref();
    })
  ]);
  let stopped = false;
  const stopping = dataSource.stop(Context.background()).then(() => {
    stopped = true;
  });
  await Promise.resolve();
  assert.equal(stopped, false, "cron datasource stopped before the pipeline result");

  await result.emit(resultContext, "done");
  await stopping;
  assert.equal(stopped, true);
});

await test("empty Croner activation waits for an explicit result", { timeout: 5_000 }, async (t) => {
  const consumerConfig = { ...streamConfig, id: 2, name: "readyAfterSkip", type: "Map" as const, idSource: streamConfig.id };
  const scheduleConfig: CronEndpointConfig = { ...endpoint, schedule: "@every 50ms" };
  const environment = makeTestEnvironment([streamConfig, consumerConfig, resultConfig], {
    dataConnectors: [{ id: 10, name: "localCron", properties: {}, type: DataConnectorType.Cron, implementation: "node/croner" }],
    endpoints: [scheduleConfig]
  });
  const input = new InputStream<string, string, Error>(streamConfig, environment, makeTestSerde(), makeTestSerde());
  const result = new ConsumedStream<string>(resultConfig, environment, makeTestSerde());
  input.setSource(result);
  const entered = Promise.withResolvers<MessageContext>();
  const received: MessageContext[] = [];
  class ReadyConsumer extends ServiceStream {
    public consume(context: MessageContext): void {
      received.push(context);
      entered.resolve(context);
    }
  }
  input.setConsumer(new ReadyConsumer(consumerConfig, environment));
  const skipped: { context: MessageContext; out: Collector<string> }[] = [];
  const invoked = Promise.withResolvers<MessageContext>();
  let calls = 0;
  makeCronEndpointConsumer(input, {
    onTrigger(context, _trigger, out) {
      calls++;
      if (calls <= 2) {
        skipped.push({ context, out });
        invoked.resolve(context);
        return;
      }
      return out.out(context, "ready");
    }
  });
  const source = environment.dataSourceById(10);
  assert.ok(source);
  try {
    await source.start(new Context(t.signal));
    const context = await invoked.promise;
    await new Promise((resolve) => setTimeout(resolve, 120));
    assert.equal(calls, 1, "missing output must not release overlap");
    assert.equal(received.length, 0);
    let stopped = false;
    const stopping = source.stop(new Context().bounded(2_000)).then(() => { stopped = true; });
    await Promise.resolve();
    assert.equal(stopped, false, "real output must still await its result");
    await result.emit(context, "done");
    await stopping;
    assert.equal(stopped, true);
  } finally {
    for (const context of received) await result.emit(context, "cleanup");
    for (const old of skipped) await result.emit(old.context, "cleanup");
    await source.stop(new Context().bounded(1_000));
  }
});

function ownerContextSource(onTrigger: ScheduleEndpointFunction<string>["onTrigger"], configuration = endpoint) {
  const environment = makeTestEnvironment([streamConfig], {
    dataConnectors: [
      {
        id: 10,
        name: "ownerCron",
        properties: {},
        type: DataConnectorType.Cron,
        implementation: "node/croner"
      }
    ],
    endpoints: [configuration]
  });
  const input = new InputStream<string, never, Error>(
    streamConfig,
    environment,
    makeTestSerde(),
    makeTestSerde()
  );
  makeCronEndpointConsumer(input, { onTrigger });
  const source = environment.dataSourceById(10);
  assert.ok(source);
  return { source, environment };
}

for (const messageOwner of [false, true]) {
  await test(
    `Croner preserves ${messageOwner ? "message" : "plain"} owner context and drains accepted work`,
    { timeout: 5_000 },
    async () => {
      const cancellation = new AbortController();
      const key = new MessageContextKey("missing");
      const owner = messageOwner
        ? new MessageContext(cancellation.signal)
            .withMetadata(new Map([["owner", "catalog"]]))
            .withLocalValue(key, "revision")
            .bounded(4_000)
        : new Context(cancellation.signal).bounded(4_000);
      const entered = Promise.withResolvers<MessageContext>();
      const release = Promise.withResolvers<undefined>();
      const { source } = ownerContextSource(async (context) => {
        entered.resolve(context);
        await release.promise;
      });
      try {
        await source.start(owner);
        const occurrence = await entered.promise;
        assert.equal(occurrence.deadline(), owner.deadline());
        assert.ok(occurrence.streamId());
        if (messageOwner) {
          assert.equal(occurrence.metadata().get("owner"), "catalog");
          assert.equal(occurrence.localValue(key), "revision");
        }
        let stopped = false;
        const stopping = source.stop(new Context().bounded(3_000)).then(() => {
          stopped = true;
        });
        await new Promise((resolve) => setTimeout(resolve, 25));
        assert.equal(stopped, false);
        assert.equal(occurrence.cancelled(), false, "scheduler stop must not cancel its owner");
        const reason = new Error("owner stopped");
        cancellation.abort(reason);
        assert.equal(occurrence.cancelled(), true);
        assert.equal(occurrence.signal().reason, reason);
        await Promise.resolve();
        assert.equal(stopped, false, "owner cancellation must not discard accepted work");
        release.resolve(undefined);
        await stopping;
        assert.equal(stopped, true);
      } finally {
        release.resolve(undefined);
        cancellation.abort();
        await source.stop(new Context().bounded(1_000));
      }
    }
  );
}

await test("Croner rejects a cancelled owner before starting the scheduler", async () => {
  const cancellation = new AbortController();
  const reason = new Error("cancelled before startup");
  cancellation.abort(reason);
  const { source } = ownerContextSource(() => {
    assert.fail("cancelled owner must not dispatch");
  });
  await assert.rejects(source.start(new Context(cancellation.signal)), (error) => error === reason);
  await source.stop(new Context());
});

await test("Croner retains a non-Error cancellation reason as the rejection cause", async () => {
  const cancellation = new AbortController();
  cancellation.abort("service shutdown");
  const { source } = ownerContextSource(() => {
    assert.fail("cancelled owner must not dispatch");
  });
  await assert.rejects(
    source.start(new Context(cancellation.signal)),
    (error: unknown) => error instanceof Error && error.cause === "service shutdown"
  );
});

await test("Croner failure logging retains the occurrence context", { timeout: 5_000 }, async (t) => {
  const logged = Promise.withResolvers<Context>();
  let occurrence: MessageContext | undefined;
  const failure = new Error("scheduled operation failed");
  const { source, environment } = ownerContextSource((context) => {
    occurrence = context;
    throw failure;
  });
  t.mock.method(environment.log(), "error", (context: Context) => {
    logged.resolve(context);
  });
  const owner = new MessageContext().withMetadata(new Map([["owner", "catalog"]]));
  try {
    await source.start(owner);
    const context = await logged.promise;
    assert.equal(context, occurrence);
    assert.equal(occurrence?.metadata().get("owner"), "catalog");
  } finally {
    await source.stop(new Context().bounded(1_000));
  }
});

await test("Croner timeout retains accepted work and permits a draining retry", { timeout: 5_000 }, async () => {
  let entered = Promise.withResolvers<MessageContext>();
  const release = Promise.withResolvers<undefined>();
  const { source } = ownerContextSource(async (context) => {
    entered.resolve(context);
    await release.promise;
  });
  const owner = new Context();
  let retry: Promise<void> | undefined;
  try {
    await source.start(owner);
    const occurrence = await entered.promise;
    await assert.rejects(source.stop(owner.bounded(5)), { name: "RuntimeDrainTimeoutError" });
    assert.equal(occurrence.cancelled(), false);
    await assert.rejects(source.start(owner), /already started/);
    let stopped = false;
    retry = source.stop(owner).then(() => { stopped = true; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(stopped, false);
    release.resolve(undefined);
    await retry;
    entered = Promise.withResolvers<MessageContext>();
    await source.start(owner);
    await entered.promise;
  } finally {
    release.resolve(undefined);
    await retry;
    await source.stop(owner);
  }
});

await test("Croner cancelled stop preserves callback ownership and the original cause", { timeout: 5_000 }, async () => {
  const entered = Promise.withResolvers<MessageContext>();
  const release = Promise.withResolvers<undefined>();
  const { source } = ownerContextSource(async (context) => {
    entered.resolve(context);
    await release.promise;
  });
  const owner = new Context();
  const cancellation = new AbortController();
  const reason = new Error("shutdown caller cancelled");
  try {
    await source.start(owner);
    const occurrence = await entered.promise;
    const stopping = source.stop(new Context(cancellation.signal));
    const rejected = assert.rejects(stopping, (error: unknown) => error === reason);
    cancellation.abort(reason);
    await rejected;
    assert.equal(occurrence.cancelled(), false);
    await assert.rejects(source.start(owner), /already started/);
  } finally {
    release.resolve(undefined);
    await source.stop(owner);
  }
});

await test("Croner startup retains failed rollback for a caller-owned retry", async (t) => {
  const { source } = ownerContextSource(() => undefined);
  const lifecycle = source.endpoint(endpoint.id) as unknown as {
    start(context: Context): void;
    stop(context: Context): Promise<void>;
  };
  const owner = new Context();
  const retry = new Context();
  const failure = new Error("scheduler setup failed");
  const calls: Context[] = [];
  t.mock.method(lifecycle, "start", () => { throw failure; });
  t.mock.method(lifecycle, "stop", (context: Context) => {
    calls.push(context);
    return context === owner ? Promise.reject(new Error("cleanup incomplete")) : Promise.resolve();
  });
  await assert.rejects(source.start(owner), (error: unknown) => error === failure);
  await assert.rejects(source.start(retry), /already started/);
  await source.stop(retry);
  assert.deepEqual(calls, [owner, retry]);
});

await test("late Croner stop cannot clear a restarted generation", async (t) => {
  const { source } = ownerContextSource(() => undefined);
  const lifecycle = source.endpoint(endpoint.id) as unknown as {
    stop(context: Context): Promise<void>;
  };
  const stop = lifecycle.stop.bind(lifecycle);
  const first = Promise.withResolvers<undefined>();
  const second = Promise.withResolvers<undefined>();
  let calls = 0;
  t.mock.method(lifecycle, "stop", async (context: Context) => {
    const call = ++calls;
    await stop(context);
    if (call === 1) await first.promise;
    if (call === 2) await second.promise;
  });
  const owner = new Context();
  await source.start(owner);
  const stopFirst = source.stop(owner);
  const stopSecond = source.stop(owner);
  try {
    first.resolve(undefined);
    await stopFirst;
    await source.start(owner);
    second.resolve(undefined);
    await stopSecond;
    await assert.rejects(source.start(owner), /already started/);
  } finally {
    first.resolve(undefined);
    second.resolve(undefined);
    await Promise.allSettled([stopFirst, stopSecond]);
    await source.stop(owner);
  }
});
