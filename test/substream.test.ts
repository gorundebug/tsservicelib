import assert from "node:assert/strict";
import { test } from "node:test";

import {
  makeMapStream,
  makeSubStream,
  type MapFunction,
  type SubStream
} from "@gorundebug/tsservicelib/operators";
import { TemporalWorkflowEnvironment } from "@gorundebug/tsservicelib/datasource/temporal/workflow";
import {
  MessageContext,
  MessageContextKey,
  SubStreamCollectorFunc,
  makeDefaultSerdeRegistry,
  noopLogger,
  noopMetrics,
  type CanonicalConfig,
  type Completion,
  type MapStreamConfig,
  type RuntimeEnvironment,
  type SubStreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

function configs(first = 1): readonly [SubStreamConfig, MapStreamConfig] {
  const common = {
    properties: {},
    pipeline: "main",
    idService: 1,
    idSources: [],
    xPos: 0,
    yPos: 0
  };
  return [
    {
      ...common,
      id: first,
      name: `Sub${String(first)}`,
      type: "SubStream",
      idSource: first + 1,
      valueType: "int32"
    },
    {
      ...common,
      id: first + 1,
      name: `Map${String(first)}`,
      type: "Map",
      idSource: first,
      valueType: "int32"
    }
  ];
}

function graph(
  environment: RuntimeEnvironment,
  body: MapFunction<number, number>,
  first = 1
): SubStream<number, number> {
  const [entryConfig, bodyConfig] = configs(first);
  const entry = makeSubStream<number, number>(entryConfig, environment);
  const result = makeMapStream(bodyConfig, entry, body);
  entry.setSource(result);
  entry.build();
  return entry;
}

const echo: MapFunction<number, number> = {
  map(context, _stream, value, out): Completion {
    return out.out(context, value);
  }
};

await test("SubStream uses normal body/result links and collector completion", async () => {
  const environment = makeTestEnvironment(configs());
  const entry = graph(environment, {
    async map(context, _stream, value, out): Promise<void> {
      for (let index = 0; index < 4; index += 1) await out.out(context, value + index);
    }
  });
  const values: number[] = [];
  const context = new MessageContext();
  await entry.consume(
    context,
    5,
    new SubStreamCollectorFunc((caller, value) => {
      assert.equal(caller, context);
      values.push(value);
      return values.length === 2;
    })
  );
  assert.deepEqual(values, [5, 6]);
  assert.equal(environment.linkCallCount(1, 2), 1);
  assert.equal(environment.linkCallCount(2, 1), 4);
  environment.validateRuntimeTopology();
});

await test("same SubStream and parent context isolate concurrent calls", async () => {
  const environment = makeTestEnvironment(configs());
  const entry = graph(environment, {
    async map(context, _stream, value, out): Promise<void> {
      await Promise.resolve(undefined);
      await out.out(context.withPriority(5), value * 10);
      await Promise.resolve(undefined);
      await out.out(context.withSampling(false), value * 10 + 1);
      await out.out(context, -1);
    }
  });
  const parent = new MessageContext().withStreamId("shared-parent");
  async function invoke(value: number): Promise<number[]> {
    const values: number[] = [];
    await entry.consume(
      parent,
      value,
      new SubStreamCollectorFunc(async (context, result) => {
        assert.equal(context, parent);
        assert.equal(context.streamId(), "shared-parent");
        values.push(result);
        await Promise.resolve(undefined);
        return values.length === 2;
      })
    );
    return values;
  }
  for (let iteration = 0; iteration < 50; iteration += 1) {
    assert.deepEqual(await Promise.all([invoke(1), invoke(2)]), [
      [10, 11],
      [20, 21]
    ]);
  }
});

await test("nested invocation of the same SubStream restores caller context", async () => {
  const environment = makeTestEnvironment(configs());
  const entry = graph(environment, {
    async map(context, _stream, value, out): Promise<void> {
      if (value === 0) {
        await out.out(context, 0);
        return;
      }
      await entry.consume(
        context,
        value - 1,
        new SubStreamCollectorFunc(async (caller, result) => {
          assert.equal(caller, context);
          await out.out(caller, result + 1);
          return true;
        })
      );
    }
  });
  const values: number[] = [];
  await entry.consume(new MessageContext(), 5, {
    out(_context, value) {
      values.push(value);
      return true;
    }
  });
  assert.deepEqual(values, [5]);
});

await test("collector can call another SubStream", async () => {
  const environment = makeTestEnvironment([...configs(), ...configs(3)]);
  const first = graph(environment, echo);
  const second = graph(environment, echo, 3);
  const values: number[] = [];
  await first.consume(
    new MessageContext(),
    7,
    new SubStreamCollectorFunc(async (context, value) => {
      await second.consume(context, value + 1, {
        out(_caller, result) {
          values.push(result);
          return true;
        }
      });
      return true;
    })
  );
  assert.deepEqual(values, [8]);
});

await test("cancellation isolates siblings and drops delayed results", async () => {
  const environment = makeTestEnvironment(configs());
  const release = Promise.withResolvers<undefined>();
  const pending: Promise<void>[] = [];
  const entry = graph(environment, {
    map(context, _stream, value, out): void {
      pending.push(release.promise.then(() => out.out(context, value)));
    }
  });
  const controller = new AbortController();
  const parent = new MessageContext().withStreamId("same-id");
  const values: number[] = [];
  const collector = new SubStreamCollectorFunc<number>((_context, value) => {
    values.push(value);
    return true;
  });
  const first = entry.consume(parent.withExternalCancellation(controller.signal), 1, collector);
  const second = entry.consume(parent, 2, collector);
  controller.abort(new Error("only first"));
  await assert.rejects(first, /only first/);
  release.resolve(undefined);
  await second;
  await Promise.all(pending);
  assert.deepEqual(values, [2]);
});

await test("callbacks serialize per call while independent calls overlap", async () => {
  const environment = makeTestEnvironment(configs());
  const pending: Promise<void>[] = [];
  const entry = graph(environment, {
    map(context, _stream, value, out): void {
      for (let index = 0; index < 3; index += 1) {
        pending.push(Promise.resolve(out.out(context, value)));
      }
    }
  });
  let active = 0;
  let maximum = 0;
  async function invoke(value: number): Promise<void> {
    let localActive = 0;
    let received = 0;
    await entry.consume(
      new MessageContext(),
      value,
      new SubStreamCollectorFunc(async (_context, result) => {
        assert.equal(result, value);
        localActive += 1;
        assert.equal(localActive, 1);
        active += 1;
        maximum = Math.max(maximum, active);
        await Promise.resolve(undefined);
        active -= 1;
        localActive -= 1;
        received += 1;
        return received === 2;
      })
    );
    assert.equal(received, 2);
  }
  await Promise.all([invoke(1), invoke(2)]);
  await Promise.all(pending);
  assert.equal(maximum, 2);
});

await test("cancellation drains active asynchronous collector", async () => {
  const environment = makeTestEnvironment(configs());
  const entry = graph(environment, echo);
  const entered = Promise.withResolvers<undefined>();
  const release = Promise.withResolvers<undefined>();
  const controller = new AbortController();
  let finished = false;
  let settled = false;
  const call = entry.consume(
    new MessageContext(controller.signal),
    1,
    new SubStreamCollectorFunc(async () => {
      entered.resolve(undefined);
      await release.promise;
      finished = true;
      return true;
    })
  );
  const rejected = assert.rejects(call, /cancelled/).then(() => {
    settled = true;
  });
  await entered.promise;
  controller.abort(new Error("cancelled"));
  await Promise.resolve(undefined);
  assert.equal(settled, false);
  release.resolve(undefined);
  await rejected;
  assert.equal(finished, true);
});

await test("collector errors propagate to Consume", async () => {
  const entry = graph(makeTestEnvironment(configs()), echo);
  await assert.rejects(
    entry.consume(
      new MessageContext(),
      1,
      new SubStreamCollectorFunc(() => {
        throw new Error("collector failure");
      })
    ),
    /collector failure/
  );
});

await test("deadline ends a call without a result", async () => {
  const entry = graph(makeTestEnvironment(configs()), { map(): void {} });
  // Keep the test process alive; MessageContext deadlines intentionally unref timers.
  const keepAlive = setTimeout(() => undefined, 1_000);
  try {
    await assert.rejects(
      entry.consume(new MessageContext().bounded(10), 1, { out: () => true }),
      /deadline/
    );
  } finally {
    clearTimeout(keepAlive);
  }
});

await test("local context bindings survive derivation without entering transport metadata", () => {
  const key = new MessageContextKey<number | undefined>(undefined);
  const other = new MessageContextKey<number | undefined>(undefined);
  const parent = new MessageContext().withStreamId("id").withLocalValue(key, 1);
  const child = parent.withLocalValue(key, 2).withPriority(9).withSampling(false);
  assert.equal(parent.localValue(key), 1);
  assert.equal(child.localValue(key), 2);
  assert.equal(child.localValue(other), undefined);
  assert.deepEqual([...child.transportMetadata()], [["x-stream-id", "id"]]);
});

function workflowEnvironment(fail = false): TemporalWorkflowEnvironment {
  const source = makeTestEnvironment(configs()).runtimeConfig().config();
  const config: CanonicalConfig = {
    ...source,
    services: source.services.map((service) => ({
      ...service,
      defaultCallSemantics: { taskPool: { poolName: "worker" } }
    })),
    pools: [{ name: "worker", executorsCount: 2, queueCapacity: 8, properties: {} }]
  };
  const environment = new TemporalWorkflowEnvironment(config, 1, makeDefaultSerdeRegistry(), {
    logger: noopLogger,
    metrics: noopMetrics
  });
  graph(
    environment,
    fail
      ? {
          map(): never {
            throw new Error("body failed");
          }
        }
      : echo
  );
  return environment;
}

await test("Temporal SubStream uses existing workflow task pool and result link", async () => {
  const environment = workflowEnvironment();
  await environment.start();
  const stream = environment.streamById(1);
  assert.ok(stream !== undefined && "consume" in stream);
  // The fixture's entry is the typed SubStream constructed above.
  const entry = stream as SubStream<number, number>;
  const values: number[] = [];
  await entry.consume(new MessageContext(), 42, {
    out(_context, value) {
      values.push(value);
      return true;
    }
  });
  await environment.finish();
  assert.deepEqual(values, [42]);
});

await test("Temporal asynchronous branch failure wakes SubStream without quiescence", async () => {
  const environment = workflowEnvironment(true);
  await environment.start();
  const entry = environment.streamById(1) as SubStream<number, number>;
  await assert.rejects(entry.consume(new MessageContext(), 1, { out: () => true }), /body failed/);
  await assert.rejects(environment.finish(), /body failed/);
});
