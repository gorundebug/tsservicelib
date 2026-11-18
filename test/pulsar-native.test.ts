import { observeCancellationListeners } from "./support/cancellation-listeners.js";
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { randomUUID } from "node:crypto";
import Pulsar from "pulsar-client";
import {
  makePulsarEndpointConsumer as makeSource,
  type EndpointHandler as SourceHandler,
  type ConsumerMessage,
  type ResultContext
} from "@gorundebug/tsservicelib/datasource/pulsar";
import {
  makePulsarEndpointConsumer as makeSink,
  makePulsarEndpointConsumerWithResult as makeSinkWithResult,
  type EndpointHandler as SinkHandler,
  type SinkMessage,
  type Delivery
} from "@gorundebug/tsservicelib/datasink/pulsar";
import {
  makeInputStream,
  makeSinkStream,
  makeSinkStreamWithResult
} from "@gorundebug/tsservicelib/operators";
import {
  Context,
  MessageContext,
  ConsumedStream,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type InputStreamConfig,
  type SinkStreamConfig,
  type StreamConfig,
  type PulsarDataConnectorConfig,
  type PulsarEndpointConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
function present<T>(value: T | undefined | null): T {
  assert.ok(value !== undefined && value !== null);
  return value;
}
const enabled = { skip: !process.env["PULSAR_TEST_URL"], timeout: 20000 };
const url = process.env["PULSAR_TEST_URL"] ?? "";
const admin = process.env["PULSAR_TEST_ADMIN_URL"] ?? "http://127.0.0.1:18080";
const ns = "public/ts-pulsar-graph-" + randomUUID(),
  topics = new Set<string>();
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function until(check: () => boolean): Promise<void> {
  const end = Date.now() + 6000;
  while (!check()) {
    if (Date.now() > end) throw new Error("condition timed out");
    await pause(10);
  }
}
async function request(method: string, path: string, value?: unknown): Promise<void> {
  const r = await fetch(admin + path, {
    method,
    headers: { "content-type": "application/json" },
    ...(value === undefined ? {} : { body: JSON.stringify(value) }),
    signal: AbortSignal.timeout(5000)
  });
  if (method === "DELETE" && r.status === 404) return;
  assert.ok(r.ok, `admin HTTP ${String(r.status)}`);
  await r.arrayBuffer();
}
function topic(): string {
  const value = `persistent://${ns}/t-${randomUUID()}`;
  topics.add(value);
  return value;
}
before(async () => {
  if (enabled.skip) return;
  await request("PUT", `/admin/v2/namespaces/${ns}`, {});
  await request("POST", `/admin/v2/namespaces/${ns}/autoTopicCreation`, {
    allowAutoTopicCreation: false
  });
  await request("POST", `/admin/v2/namespaces/${ns}/autoSubscriptionCreation`, {
    allowAutoSubscriptionCreation: false
  });
});
after(async () => {
  if (enabled.skip) return;
  for (const t of topics)
    await request(
      "DELETE",
      "/admin/v2/persistent/" + t.slice("persistent://".length) + "?force=true"
    );
  await request("DELETE", `/admin/v2/namespaces/${ns}`);
});
const inputConfig: InputStreamConfig = {
  id: 1,
  name: "input",
  type: "Input",
  pipeline: "main",
  properties: {},
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  idEndpoint: 100,
  valueType: "string"
};
const resultConfig: StreamConfig = {
  ...inputConfig,
  id: 3,
  name: "result",
  type: "Map",
  idSource: 1
};
const sinkConfig: SinkStreamConfig = {
  ...inputConfig,
  id: 2,
  name: "output",
  type: "Sink",
  idSource: 1,
  idEndpoint: 101
};
function config(url: string, kind: 11): PulsarDataConnectorConfig {
  return {
    id: 10,
    name: "broker",
    type: kind,
    implementation: "pulsar-client-node",
    properties: {},
    servers: [url],
    adminUrl: admin,
    connectTimeoutMs: 1000,
    operationTimeoutMs: 2000
  };
}
function endpoint(
  kind: 11,
  source: boolean,
  overrides: Partial<PulsarEndpointConfig> = {}
): PulsarEndpointConfig {
  return {
    id: source ? 100 : 101,
    name: source ? "receive" : "publish",
    idDataConnector: 10,
    properties: {},
    topic: "persistent://public/unused/events",
    enabled: true,
    ...(source
      ? { subscriptionName: "workers", createSubscription: true, createTopic: true }
      : { createTopic: true }),
    ...overrides
  };
}
class Recording extends ServiceStream implements TypedStreamConsumer<string> {
  public values: string[] = [];
  public consume(_c: MessageContext, value: string): void {
    this.values.push(value);
  }
}
function sourceHarness(
  url: string,
  kind: 11,
  handler: SourceHandler<undefined, string, string, Error>,
  ep = endpoint(kind, true),
  results = false,
  connection: Partial<PulsarDataConnectorConfig> = {}
) {
  const metrics = new TestMetrics();
  const env = makeTestEnvironment([inputConfig, resultConfig], {
    metrics,
    dataConnectors: [{ ...config(url, kind), ...connection }],
    endpoints: [ep]
  });
  env.serdeRegistry().registerStreamErrorType(1, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, env);
  const output = new ConsumedStream(resultConfig, env, env.serde(stringSerdeType));
  if (results) input.setSource(output);
  const record = new Recording(resultConfig, env);
  if (!results) input.setConsumer(record);
  makeSource(input, handler);
  return { metrics, count: (observation: string) => metrics.counterValue("broker_endpoint_source_observations_total", {protocol: "pulsar", connector: config(url, kind).name, endpoint: ep.name, observation}), env, input, output, record, source: present(env.dataSourceById(10)) };
}
function sourceHandler(
  consume: (
    c: MessageContext,
    sc: StreamContext<string, string, Error>,
    m: ConsumerMessage,
    r: ResultContext<undefined, string, string, Error>
  ) => void | Promise<void>
): SourceHandler<undefined, string, string, Error> & {
  failures: Error[];
  ended: (Error | undefined)[];
} {
  const failures: Error[] = [];
  const ended: (Error | undefined)[] = [];
  return {
    failures,
    ended,
    onError: (_c, _sc, error) => {
      failures.push(error);
    },
    beginRequest: (c) => ({ context: c, state: undefined }),
    consumeMessage: (c, sc, _s, m, r) => consume(c, sc, m, r),
    getMessageId: () => "one",
    endRequest: (_c, _sc, error) => {
      ended.push(error);
    }
  };
}
function sinkHarness(
  url: string,
  kind: 11,
  consume: (c: MessageContext, message: SinkMessage<never>, value: string) => void | Promise<void>,
  ep = endpoint(kind, false),
  connection: Partial<PulsarDataConnectorConfig> = {}
) {
  const upstreamConfig: StreamConfig = { ...inputConfig, type: "Map" };
  const env = makeTestEnvironment([upstreamConfig, sinkConfig], {
    dataConnectors: [{ ...config(url, kind), ...connection }],
    endpoints: [ep]
  });
  env.serdeRegistry().registerStreamErrorType(2, errorSerdeType);
  const upstream = new ConsumedStream(upstreamConfig, env, env.serde(stringSerdeType));
  const output = makeSinkStream<string, Error>(sinkConfig, upstream);
  const errors: (Error | undefined)[] = [];
  const handler: SinkHandler<undefined, string, never, Error> = {
    getStreamId: (c) => c.streamId() ?? "publication",
    beginRequest: (c) => ({ context: c, state: undefined }),
    consumeMessage: (c, _sc, _s, value, message) => consume(c, message, value),
    endRequest: (_c, _sc, error) => {
      errors.push(error);
    }
  };
  makeSink(output, handler);
  return { env, upstream, errors, sink: present(env.dataSinkById(10)) };
}

for (const ack of [false, true])
  void test(
    `Pulsar native graph correlation Done versus ACK ${String(ack)}`,
    enabled,
    async (t) => {
      const address = topic(),
        received: ConsumerMessage[] = [],
        outcomes: Delivery[] = [];
      const handler = sourceHandler(async (ctx, sc, m, result) => {
        received.push(m);
        assert.equal(ctx.streamId(), "event");
        assert.equal(
          ctx.metadata().get("traceparent"),
          "00-11111111111111111111111111111111-2222222222222222-01"
        );
        result.setResultCallback("one", async () => {
          if (ack) await m.ack();
          result.done();
          return true;
        });
        await sc.collect(ctx, "payload");
      });
      const source = sourceHarness(url, 11, handler, endpoint(11, true, { topic: address }), true);
      class Results extends ServiceStream implements TypedStreamConsumer<string> {
        public consume(c: MessageContext, value: string) {
          return source.output.emit(c, value);
        }
      }
      source.input.setConsumer(new Results(resultConfig, source.env));
      const sink = sinkHarness(
        url,
        11,
        (ctx, m) => {
          const bytes = Buffer.from([0, 255]);
          m.message = {
            data: bytes,
            properties: {
              caller: "retained",
              TraceParent: "00-11111111111111111111111111111111-2222222222222222-01"
            }
          };
          m.send(ctx, (d) => {
            outcomes.push(present(d));
          });
          bytes.fill(3);
        },
        endpoint(11, false, { topic: address })
      );
      t.after(async () => {
        await source.source.stop(new Context().bounded(2000));
        await sink.sink.stop(new Context().bounded(2000));
      });
      await source.source.start(new Context());
      await sink.sink.start(new Context());
      await sink.upstream.emit(new MessageContext().withStreamId("event"), "value");
      await until(() => handler.ended.length === 1 && outcomes.length === 1);
      assert.equal(handler.ended[0], undefined);
      assert.equal(source.count("received"), 1);
      assert.equal(source.count("redelivered"), 0);
      assert.deepEqual(present(received[0]).message.getData(), Buffer.from([0, 255]));
      assert.equal(present(received[0]).message.getProperties()["caller"], "retained");
      assert.deepEqual(
        present(received[0]).message.getMessageId().serialize(),
        present(present(outcomes[0]).id).serialize()
      );
      await assert.rejects(present(received[0]).ack(), /expired/);
      await source.source.stop(new Context().bounded(2000));
      const client = new Pulsar.Client({ serviceUrl: url, operationTimeoutSeconds: 2 });
      t.after(() => client.close());
      const consumer = await client.subscribe({
        topic: address,
        subscription: "workers",
        subscriptionType: "Shared",
        receiverQueueSize: 1
      });
      if (!ack) {
        const pending = await consumer.receive();
        assert.deepEqual(
          pending.getMessageId().serialize(),
          present(received[0]).message.getMessageId().serialize()
        );
        await consumer.acknowledge(pending);
      } else {
        let delivered = false;
        const pending = consumer.receive().then(
          () => {
            delivered = true;
          },
          () => undefined
        );
        await pause(100);
        await consumer.close();
        await pending;
        assert.equal(delivered, false);
        return;
      }
      await consumer.close();
    }
  );

void test("Pulsar callback capacity, retained handles and drain", enabled, async (t) => {
  let retained: SinkMessage<never> | undefined,
    heldContext: MessageContext | undefined,
    entered = false,
    release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const outcomes: Delivery[] = [];
  const sink = sinkHarness(
    url,
    11,
    (context, m, value) => {
      if (value === "retain") {
        retained = m;
        heldContext = context;
        return;
      }
      m.message = { data: Buffer.from(value) };
      m.send(context, async (d) => {
        entered = true;
        await gate;
        outcomes.push(present(d));
      });
    },
    endpoint(11, false, { topic: topic(), publisherMaxInFlight: 1 })
  );
  t.after(() => {
    release();
    return sink.sink.stop(new Context().bounded(2000));
  });
  await sink.sink.start(new Context());
  await sink.upstream.emit(new MessageContext(), "retain");
  await sink.upstream.emit(new MessageContext(), "sent");
  await until(() => entered);
  assert.throws(() => {
    present(retained).send(present(heldContext), () => undefined);
  }, /retained/);
  await assert.rejects(present(retained).sendSync(present(heldContext)), /retained/);
  await sink.upstream.emit(new MessageContext(), "overflow");
  assert.match(present(sink.errors.at(-1)).message, /admission/);
  let stopped = false;
  const stop = sink.sink.stop(new Context().bounded(2000)).then(() => {
    stopped = true;
  });
  await pause(30);
  assert.equal(stopped, false);
  release();
  await stop;
  assert.equal(outcomes.length, 1);
  assert.equal(outcomes[0]?.outcome, "Accepted");
});

void test(
  "Pulsar typed async/sync/skip outcomes preserve result and error routes",
  enabled,
  async (t) => {
    const upstreamConfig: StreamConfig = { ...inputConfig, type: "Map" };
    const env = makeTestEnvironment([upstreamConfig, sinkConfig, resultConfig], {
      dataConnectors: [config(url, 11)],
      endpoints: [endpoint(11, false, { topic: topic() })]
    });
    env.serdeRegistry().registerStreamErrorType(2, errorSerdeType);
    const upstream = new ConsumedStream(upstreamConfig, env, env.serde(stringSerdeType));
    const output = makeSinkStreamWithResult<string, string, Error>(sinkConfig, upstream),
      record = new Recording(resultConfig, env);
    output.setConsumer(record);
    const errors: Error[] = [];
    class Errors extends ServiceStream implements TypedStreamConsumer<Error> {
      public consume(_c: MessageContext, e: Error) {
        errors.push(e);
      }
    }
    output.errorStream().setConsumer(new Errors({ ...resultConfig, id: 4, name: "errors" }, env));
    makeSinkWithResult(output, {
      getStreamId: () => "correlation",
      beginRequest: (c) => ({ context: c, state: undefined }),
      consumeMessage: async (c, _sc, _state, value, m) => {
        m.message = { data: Buffer.from(value) };
        if (value === "skip") await m.skip(c, "skipped");
        else if (value === "error") throw new Error("business error");
        else if (value === "sync") {
          const d = await m.sendSync(c);
          await m.out(c, d.outcome);
        } else m.send(c, (d) => d?.outcome ?? "missing");
      },
      endRequest: (c, sc, error) => {
        if (error) return sc.errorCollect(c, error);
      }
    });
    const sink = present(env.dataSinkById(10));
    t.after(() => sink.stop(new Context().bounded(2000)));
    await sink.start(new Context());
    for (const value of ["async", "sync", "skip", "error"])
      await upstream.emit(new MessageContext(), value);
    await sink.stop(new Context().bounded(2000));
    assert.equal(record.values.filter((v) => v === "Accepted").length, 2);
    assert.ok(record.values.includes("skipped"));
    assert.equal(errors[0]?.message, "business error");
  }
);

void test(
  "Pulsar Stop deadline cancels callback context and permits owned cleanup",
  enabled,
  async (t) => {
    let entered = false,
      release!: () => void,
      callbackContext: MessageContext | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const sink = sinkHarness(
      url,
      11,
      (c, m) => {
        m.message = { data: Buffer.from("held") };
        m.send(c, async () => {
          callbackContext = c;
          entered = true;
          await gate;
        });
      },
      endpoint(11, false, { topic: topic() })
    );
    t.after(async () => {
      release();
      await sink.sink.stop(new Context().bounded(2000));
    });
    await sink.sink.start(new Context());
    await sink.upstream.emit(new MessageContext(), "held");
    await until(() => entered);
    const started = performance.now();
    await assert.rejects(sink.sink.stop(new Context().bounded(50)), /deadline/);
    assert.ok(performance.now() - started < 1000);
    assert.equal(present(callbackContext).cancelled(), true);
    release();
    await sink.sink.stop(new Context().bounded(2000));
  }
);

void test(
  "Pulsar source Stop drains admitted handler with ACK still available",
  enabled,
  async (t) => {
    let entered = false,
      release!: () => void,
      acked = false;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const address = topic();
    const handler = sourceHandler(async (_ctx, _sc, m) => {
      entered = true;
      await gate;
      await m.ack();
      acked = true;
    });
    const source = sourceHarness(url, 11, handler, endpoint(11, true, { topic: address }));
    const sink = sinkHarness(
      url,
      11,
      (c, m) => {
        m.message = { data: Buffer.from("drain") };
        m.send(c, () => undefined);
      },
      endpoint(11, false, { topic: address })
    );
    t.after(async () => {
      release();
      await source.source.stop(new Context().bounded(2000));
      await sink.sink.stop(new Context().bounded(2000));
    });
    await source.source.start(new Context());
    await sink.sink.start(new Context());
    await sink.upstream.emit(new MessageContext(), "drain");
    await until(() => entered);
    let stopped = false;
    const stopping = source.source.stop(new Context().bounded(2000)).then(() => {
      stopped = true;
    });
    await pause(30);
    assert.equal(stopped, false);
    release();
    await stopping;
    assert.equal(acked, true);
    assert.equal(handler.ended[0], undefined);
  }
);

await test("pulsar completed requests retain bounded generation cancellation listeners", { ...enabled, timeout: 60000 }, async (t) => {
    const observation = observeCancellationListeners();
    t.after(observation.restore);
    const address = topic();
    const handler = sourceHandler(async (_ctx, _sc, m, result) => { await m.ack(); result.done(); });
    const source = sourceHarness(url, 11, handler, endpoint(11, true, { topic: address }), true);
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    const peer = new Pulsar.Client({ serviceUrl: url });
    const publisher = await peer.createProducer({ topic: address });
    t.after(async () => { await publisher.close(); await peer.close(); });
    for (let i = 0; i < 1024; i++) {
      await publisher.send({ data: Buffer.from(String(i)) });
      await until(() => handler.ended.length === i + 1);
      if (i === 127 || i === 1023) observation.assertBounded();
    }
    assert.equal(handler.failures.length, 0);
    await source.source.stop(new Context().bounded(1000));
    observation.assertBounded();
});
