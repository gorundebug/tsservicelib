import { observeCancellationListeners } from "./support/cancellation-listeners.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { createServer, connect, type Socket } from "node:net";
import { createServer as createTLSServer } from "node:tls";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createEnvironment, type Connection } from "rabbitmq-amqp-js-client";
import {
  makeRabbitMQEndpointConsumer as makeSource,
  type EndpointHandler as SourceHandler,
  type ConsumerMessage,
  type ResultContext
} from "@gorundebug/tsservicelib/datasource/rabbitmq";
import {
  makeRabbitMQEndpointConsumer as makeSink,
  makeRabbitMQEndpointConsumerWithResult as makeSinkWithResult,
  type EndpointHandler as SinkHandler,
  type SinkMessage,
  type Delivery
} from "@gorundebug/tsservicelib/datasink/rabbitmq";
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
  type RabbitMQDataConnectorConfig,
  type RabbitMQEndpointConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
function present<T>(v: T | undefined | null): T {
  assert.ok(v !== undefined && v !== null);
  return v;
}
const url = process.env["RABBITMQ_TEST_URL"] ?? "";
const enabled = { skip: !url, timeout: 15000 };
const pause = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
async function until(check: () => boolean): Promise<void> {
  const deadline = Date.now() + 6000;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("condition timed out");
    await pause(10);
  }
}
async function admin(): Promise<Connection> {
  const u = new URL(url);
  return createEnvironment({
    host: u.hostname,
    port: Number(u.port || 5672),
    username: "adapter",
    password: "adapter-test"
  }).createConnection({ reconnect: false });
}
function name(): string {
  return "ts-adapter-" + randomUUID();
}
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
function config(url: string, kind: 9): RabbitMQDataConnectorConfig {
  return {
    id: 10,
    name: "broker",
    type: kind,
    implementation: "rabbitmq-amqp-js-client",
    properties: {},
    servers: [url],
    username: "adapter",
    password: "adapter-test",
    connectTimeoutMs: 1000,
    operationTimeoutMs: 2000
  };
}
function endpoint(
  kind: 9,
  source: boolean,
  overrides: Partial<RabbitMQEndpointConfig> = {}
): RabbitMQEndpointConfig {
  return {
    id: source ? 100 : 101,
    name: source ? "receive" : "publish",
    idDataConnector: 10,
    properties: {},
    queueName: "unused",
    enabled: true,
    createQueue: false,
    createExchange: false,
    createBinding: false,
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
  kind: 9,
  handler: SourceHandler<undefined, string, string, Error>,
  ep = endpoint(kind, true),
  results = false,
  connection: Partial<RabbitMQDataConnectorConfig> = {}
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
  return { metrics, count: (observation: string) => metrics.counterValue("broker_endpoint_source_observations_total", {protocol: "rabbitmq", connector: config(url, kind).name, endpoint: ep.name, observation}), env, input, output, record, source: present(env.dataSourceById(10)) };
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
  kind: 9,
  consume: (c: MessageContext, message: SinkMessage<never>, value: string) => void | Promise<void>,
  ep = endpoint(kind, false),
  connection: Partial<RabbitMQDataConnectorConfig> = {}
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

await test("RabbitMQ native async accepted, metadata and explicit accept", enabled, async (t) => {
  const a = await admin();
  const q = name();
  await a.management().declareQueue(q, { type: "quorum" });
  t.after(async () => {
    await a.management().deleteQueue(q);
    await a.close();
  });
  const deliveries: Delivery[] = [];
  let received = false;
  const handler = sourceHandler(async (c, sc, m) => {
    assert.equal(
      Buffer.from(
        (m.message.body as { content?: Buffer }).content ?? (m.message.body as Buffer)
      ).toString(),
      "hello"
    );
    assert.equal(m.message.application_properties?.["Caller"], "kept");
    assert.equal(c.streamId(), "trace-id");
    await m.accept();
    received = true;
  });
  const source = sourceHarness(url, 9, handler, endpoint(9, true, { queueName: q }));
  const sink = sinkHarness(
    url,
    9,
    (c, m, v) => {
      m.message = { body: Buffer.from(v), application_properties: { Caller: "kept" } };
      m.send(c, (d) => {
        deliveries.push(present(d));
      });
    },
    endpoint(9, false, { queueName: q })
  );
  t.after(async () => {
    await source.source.stop(new Context().bounded(1000));
    await sink.sink.stop(new Context().bounded(1000));
  });
  await source.source.start(new Context());
  await sink.sink.start(new Context());
  await sink.upstream.emit(new MessageContext().withStreamId("trace-id"), "hello");
  await until(() => received && deliveries.length === 1);
  assert.equal(deliveries[0]?.outcome, "accepted");
  assert.deepEqual(sink.errors, [undefined]);
});

await test(
  "RabbitMQ release redelivers, reject removes, Done never ACKs and expired handles fail",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    const p = await a.createPublisher({ queue: { name: q } });
    await p.publish({ body: Buffer.from("redeliver") });
    let held: ConsumerMessage | undefined;
    let count = 0;
    const handler = sourceHandler(async (_c, _sc, m, r) => {
      held = m;
      count++;
      if (count === 1) await m.release();
      r.done();
    });
    const first = sourceHarness(url, 9, handler, endpoint(9, true, { queueName: q }), true);
    await first.source.start(new Context());
    await until(() => count === 2);
    await until(() => handler.ended.length === 2);
    assert.equal(first.count("received"), 2);
    // AMQP released does not increment native delivery-count.
    assert.equal(first.count("redelivered"), 0);
    await assert.rejects(() => present(held).accept(), /expired/);
    await first.source.stop(new Context().bounded(1000));
    let rejected = false;
    const second = sourceHarness(
      url,
      9,
      sourceHandler(async (_c, _sc, m) => {
        await m.reject();
        rejected = true;
      }),
      endpoint(9, true, { queueName: q })
    );
    t.after(() => second.source.stop(new Context().bounded(1000)));
    await second.source.start(new Context());
    await until(() => rejected);
    await pause(30);
    assert.equal((await a.management().getQueueInfo(q)).getInfo.messageCount, 0);
  }
);

await test(
  "RabbitMQ callbacks register before synchronous results; typed lifecycle",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    let callbacks = 0;
    const handler = sourceHandler(async (c, sc, m, r) => {
      r.setResultCallback("one", async () => {
        callbacks++;
        await m.accept();
        r.done();
        return true;
      });
      await sc.collect(c, "value");
    });
    const source = sourceHarness(url, 9, handler, endpoint(9, true, { queueName: q }), true);
    class ReturnResult extends ServiceStream implements TypedStreamConsumer<string> {
      public consume(c: MessageContext, v: string) {
        return source.output.emit(c, v);
      }
    }
    source.input.setConsumer(new ReturnResult(resultConfig, source.env));
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    const p = await a.createPublisher({ queue: { name: q } });
    await p.publish({ body: Buffer.from("body") });
    await until(() => handler.ended.length === 1);
    assert.equal(callbacks, 1);
    assert.equal(handler.ended[0], undefined);
  }
);

await test(
  "RabbitMQ existing-only fails without provisioning; explicit queue/exchange/binding and unroutable",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name(),
      exchange = name();
    t.after(async () => {
      await a
        .management()
        .deleteQueue(q)
        .catch(() => undefined);
      await a
        .management()
        .deleteExchange(exchange)
        .catch(() => undefined);
      await a.close();
    });
    const missing = sinkHarness(url, 9, () => undefined, endpoint(9, false, { queueName: q }));
    await assert.rejects(() => missing.sink.start(new Context().bounded(2000)));
    await assert.rejects(() => a.management().getQueueInfo(q));
    const outcomes: Delivery[] = [];
    const sink = sinkHarness(
      url,
      9,
      async (c, m) => {
        m.message = { body: Buffer.from("bound") };
        outcomes.push(await m.sendSync(c));
      },
      endpoint(9, false, {
        queueName: q,
        exchangeName: exchange,
        exchangeType: "direct",
        routingKey: "yes",
        createQueue: true,
        createExchange: true,
        createBinding: true
      })
    );
    t.after(() => sink.sink.stop(new Context().bounded(1000)));
    await sink.sink.start(new Context());
    await sink.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[0]?.outcome, "accepted");
    const noRoute = sinkHarness(
      url,
      9,
      async (c, m) => {
        outcomes.push(await m.sendSync(c));
      },
      endpoint(9, false, { queueName: undefined, exchangeName: exchange, routingKey: "no" })
    );
    t.after(() => noRoute.sink.stop(new Context().bounded(1000)));
    await noRoute.sink.start(new Context());
    await noRoute.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[1]?.outcome, "unroutable");
    const mismatch = sinkHarness(
      url,
      9,
      () => undefined,
      endpoint(9, false, { queueName: q, queueType: "classic" })
    );
    await assert.rejects(() => mismatch.sink.start(new Context().bounded(1000)));
  }
);

await test(
  "RabbitMQ callback capacity survives EndRequest; stop deadline is bounded",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let callbacks = 0;
    const sink = sinkHarness(
      url,
      9,
      (c, m) => {
        m.send(c, async (d) => {
          assert.equal(d?.outcome, "accepted");
          callbacks++;
          await gate;
        });
      },
      endpoint(9, false, { queueName: q, publisherMaxInFlight: 1 })
    );
    await sink.sink.start(new Context());
    await sink.upstream.emit(new MessageContext(), "one");
    await until(() => callbacks === 1);
    assert.deepEqual(sink.errors, [undefined]);
    await sink.upstream.emit(new MessageContext(), "two");
    assert.match(String(sink.errors[1]), /admission limit/);
    const started = Date.now();
    await assert.rejects(() => sink.sink.stop(new Context().bounded(60)));
    assert.ok(Date.now() - started < 500);
    release();
  }
);

await test(
  "RabbitMQ source stop drains admitted handler and settlement before EndRequest",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let entered = false,
      settled = false;
    const h = sourceHandler(async (_c, _sc, m) => {
      entered = true;
      await gate;
      await m.accept();
      settled = true;
    });
    const s = sourceHarness(url, 9, h, endpoint(9, true, { queueName: q }));
    await s.source.start(new Context());
    const p = await a.createPublisher({ queue: { name: q } });
    await p.publish({ body: Buffer.from("wait") });
    await until(() => entered);
    const stopped = s.source.stop(new Context().bounded(1500));
    await pause(30);
    assert.equal(h.ended.length, 0);
    release();
    await stopped;
    assert.ok(settled);
    assert.deepEqual(h.ended, [undefined]);
  }
);

async function proxy(tls?: { key: Buffer; cert: Buffer }) {
  const sockets = new Set<Socket>();
  let blockReplies = false;
  const bridge = (front: Socket): void => {
    const upstream = new URL(url),
      back = connect({ host: upstream.hostname, port: Number(upstream.port || 5672) });
    sockets.add(front);
    sockets.add(back);
    front.on("data", (data) => {
      if (!back.write(data)) front.pause();
    });
    back.on("drain", () => front.resume());
    back.on("data", (data) => {
      if (!blockReplies && !front.write(data)) back.pause();
    });
    front.on("drain", () => back.resume());
    front.on("error", () => back.destroy());
    back.on("error", () => front.destroy());
    front.on("close", () => {
      sockets.delete(front);
      back.destroy();
    });
    back.on("close", () => {
      sockets.delete(back);
      front.destroy();
    });
  };
  const server = tls ? createTLSServer(tls, bridge) : createServer(bridge);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return {
    url: `${tls ? "amqps://localhost" : "amqp://127.0.0.1"}:${String(address.port)}`,
    block: () => {
      blockReplies = true;
    },
    unblock: () => {
      blockReplies = false;
    },
    drop: () => {
      for (const s of sockets) s.destroy();
    },
    close: async () => {
      for (const s of sockets) s.destroy();
      await new Promise<void>((r) =>
        server.close(() => {
          r();
        })
      );
    }
  };
}
await test(
  "RabbitMQ source reconnect redelivers and fences old native handle",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name(),
      bridge = await proxy();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await bridge.close();
      await a.management().deleteQueue(q);
      await a.close();
    });
    let first: ConsumerMessage | undefined,
      received = 0;
    const h = sourceHandler(async (_c, _sc, m, r) => {
      received++;
      if (received === 1) first = m;
      else {
        await m.accept();
        r.done();
      }
    });
    const s = sourceHarness(bridge.url, 9, h, endpoint(9, true, { queueName: q }), true);
    t.after(() => s.source.stop(new Context().bounded(1000)));
    await s.source.start(new Context());
    const p = await a.createPublisher({ queue: { name: q } });
    await p.publish({ body: Buffer.from("recover") });
    await until(() => received === 1);
    bridge.drop();
    await until(() => received === 2 && h.ended.length === 2);
    await assert.rejects(() => present(first).accept(), /expired/);
    assert.ok(h.failures.length >= 1);
    assert.ok(h.ended[0] instanceof Error);
  }
);
await test(
  "RabbitMQ lost confirmation is unknown and never replayed; subsequent sends recover",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name(),
      bridge = await proxy();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await bridge.close();
      await a.management().deleteQueue(q);
      await a.close();
    });
    const outcomes: Delivery[] = [];
    const s = sinkHarness(
      bridge.url,
      9,
      (c, m) => {
        m.send(c, (d) => {
          outcomes.push(present(d));
        });
      },
      endpoint(9, false, { queueName: q }),
      { operationTimeoutMs: 200 }
    );
    t.after(() => s.sink.stop(new Context().bounded(1000)));
    await s.sink.start(new Context());
    bridge.block();
    await s.upstream.emit(new MessageContext(), "lost");
    await until(() => outcomes.length === 1);
    assert.equal(outcomes[0]?.outcome, "unknown");
    assert.equal((await a.management().getQueueInfo(q)).getInfo.messageCount, 1);
    bridge.unblock();
    await pause(500);
    await s.upstream.emit(new MessageContext(), "next");
    await until(() => outcomes.length === 2);
    assert.equal(outcomes[1]?.outcome, "accepted");
    assert.equal((await a.management().getQueueInfo(q)).getInfo.messageCount, 2);
  }
);
await test(
  "RabbitMQ cancellation before admission is unsent; cancellation after native send is unknown",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name(),
      bridge = await proxy();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await bridge.close();
      await a.management().deleteQueue(q);
      await a.close();
    });
    const outcomes: Delivery[] = [];
    const s = sinkHarness(
      bridge.url,
      9,
      (c, m) => {
        m.send(c, (d) => {
          outcomes.push(present(d));
        });
      },
      endpoint(9, false, { queueName: q })
    );
    t.after(() => s.sink.stop(new Context().bounded(1000)));
    await s.sink.start(new Context());
    const cancelled = new AbortController();
    cancelled.abort(new Error("cancel before"));
    await assert.rejects(async () =>
      s.upstream.emit(new MessageContext(cancelled.signal), "unsent")
    );
    assert.equal(outcomes.length, 0);
    bridge.block();
    const after = new AbortController();
    await s.upstream.emit(new MessageContext(after.signal), "sent");
    await pause(50);
    after.abort(new Error("cancel after"));
    await until(() => outcomes.length === 1);
    assert.equal(outcomes[0]?.outcome, "unknown");
    assert.equal((await a.management().getQueueInfo(q)).getInfo.messageCount, 1);
  }
);
await test(
  "RabbitMQ startup cancellation and bad credentials are bounded and sanitized",
  enabled,
  async (t) => {
    const bridge = await proxy();
    t.after(() => bridge.close());
    bridge.block();
    const s = sinkHarness(
      bridge.url,
      9,
      () => undefined,
      endpoint(9, false, { queueName: name() }),
      {
        connectTimeoutMs: 100,
        operationTimeoutMs: 2000
      }
    );
    let started = Date.now();
    await assert.rejects(() => s.sink.start(new Context()), /deadline/);
    assert.ok(Date.now() - started < 700);
    const bad = sinkHarness(url, 9, () => undefined, endpoint(9, false, { queueName: name() }), {
      password: "SECRET",
      connectTimeoutMs: 200
    });
    started = Date.now();
    await assert.rejects(
      () => bad.sink.start(new Context()),
      (error) => error instanceof Error && !error.message.includes("SECRET")
    );
    assert.ok(Date.now() - started < 800);
  }
);
await test(
  "RabbitMQ source saturation is observable, does not ACK, and stop deadline bounds handlers",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    let finish!: () => void;
    const gate = new Promise<void>((r) => {
      finish = r;
    });
    let entered = false;
    const h = sourceHandler(async () => {
      entered = true;
      await gate;
    });
    const s = sourceHarness(url, 9, h, endpoint(9, true, { queueName: q, consumerCredits: 1 }));
    await s.source.start(new Context());
    const p = await a.createPublisher({ queue: { name: q } });
    await p.publish({ body: Buffer.from("one") });
    await until(() => entered);
    await p.publish({ body: Buffer.from("two") });
    await until(() => h.failures.length > 0);
    const started = Date.now();
    await assert.rejects(() => s.source.stop(new Context().bounded(60)));
    assert.ok(Date.now() - started < 500);
    assert.equal(h.ended.length, 0);
    finish();
    await until(() => h.ended.length === 1);
    assert.equal((await a.management().getQueueInfo(q)).getInfo.messageCount, 2);
  }
);
await test("RabbitMQ trusted TLS works and untrusted peer fails", enabled, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "ts-rabbit-tls-")),
    cert = join(directory, "cert.pem"),
    key = join(directory, "key.pem");
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      key,
      "-out",
      cert,
      "-days",
      "1",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost"
    ],
    { stdio: "ignore" }
  );
  const bridge = await proxy({ key: await readFile(key), cert: await readFile(cert) }),
    a = await admin(),
    q = name();
  await a.management().declareQueue(q, { type: "quorum" });
  t.after(async () => {
    await bridge.close();
    await a.management().deleteQueue(q);
    await a.close();
    await rm(directory, { recursive: true, force: true });
  });
  const outcomes: Delivery[] = [];
  const good = sinkHarness(
    bridge.url,
    9,
    async (c, m) => {
      outcomes.push(await m.sendSync(c));
    },
    endpoint(9, false, { queueName: q }),
    { tlsCaFile: cert }
  );
  t.after(() => good.sink.stop(new Context().bounded(1000)));
  await good.sink.start(new Context());
  await good.upstream.emit(new MessageContext(), "tls");
  assert.equal(outcomes[0]?.outcome, "accepted");
  const bad = sinkHarness(bridge.url, 9, () => undefined, endpoint(9, false, { queueName: q }), {
    connectTimeoutMs: 300
  });
  await assert.rejects(() => bad.sink.start(new Context()));
});

await test(
  "RabbitMQ typed async/sync/skip results preserve ordinary E routing",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    const upstreamConfig: StreamConfig = { ...inputConfig, type: "Map" };
    const env = makeTestEnvironment([upstreamConfig, sinkConfig, resultConfig], {
      dataConnectors: [config(url, 9)],
      endpoints: [endpoint(9, false, { queueName: q })]
    });
    env.serdeRegistry().registerStreamErrorType(2, errorSerdeType);
    const upstream = new ConsumedStream(upstreamConfig, env, env.serde(stringSerdeType));
    const output = makeSinkStreamWithResult<string, string, Error>(sinkConfig, upstream);
    const recorded = new Recording(resultConfig, env);
    output.setConsumer(recorded);
    const errors: Error[] = [];
    class Errors extends ServiceStream implements TypedStreamConsumer<Error> {
      public consume(_c: MessageContext, error: Error): void {
        errors.push(error);
      }
    }
    output.errorStream().setConsumer(new Errors({ ...resultConfig, id: 4, name: "errors" }, env));
    const handler: SinkHandler<undefined, string, string, Error> = {
      getStreamId: () => "caller",
      beginRequest: (c) => ({ context: c, state: undefined }),
      consumeMessage: async (c, _sc, _s, value, message) => {
        message.message = { body: Buffer.from(value) };
        if (value === "skip") await message.skip(c, "skipped");
        else if (value === "sync") {
          const d = await message.sendSync(c);
          await message.out(c, `sync:${d.outcome}`);
        } else if (value === "error") throw new Error("typed business error");
        else message.send(c, (d, e) => e?.message ?? `async:${String(d?.outcome)}`);
      },
      endRequest: (c, sc, error) => {
        if (error) return sc.errorCollect(c, error);
      }
    };
    makeSinkWithResult(output, handler);
    const sink = present(env.dataSinkById(10));
    t.after(() => sink.stop(Context.background().bounded(500)));
    await sink.start(Context.background());
    for (const value of ["async", "sync", "skip", "error"])
      await upstream.emit(new MessageContext(), value);
    await sink.stop(Context.background().bounded(1000));
    assert.ok(recorded.values.includes("async:accepted"));
    assert.ok(recorded.values.includes("sync:accepted"));
    assert.ok(recorded.values.includes("skipped"));
    assert.equal(errors[0]?.message, "typed business error");
    assert.equal((await a.management().getQueueInfo(q)).getInfo.messageCount, 2);
  }
);
await test(
  "RabbitMQ native rejected outcome retains disposition for full quorum queue",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, {
      type: "quorum",
      arguments: { "x-max-length": 1 as unknown as string, "x-overflow": "reject-publish" }
    });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    const outcomes: Delivery[] = [];
    const s = sinkHarness(
      url,
      9,
      async (c, m) => {
        m.message = { body: Buffer.from("payload"), durable: true };
        outcomes.push(await m.sendSync(c));
      },
      endpoint(9, false, { queueName: q })
    );
    t.after(() => s.sink.stop(new Context().bounded(1000)));
    await s.sink.start(new Context());
    await s.upstream.emit(new MessageContext(), "one");
    // Quorum overflow notification can lag the accepted publication.
    for (let i = 0; i < 20 && !outcomes.some((d) => d.outcome === "rejected"); i++) {
      await pause(50);
      await s.upstream.emit(new MessageContext(), "overflow");
    }
    assert.equal(outcomes[0]?.outcome, "accepted");
    const rejected = present(outcomes.find((d) => d.outcome === "rejected"));
    assert.equal(rejected.nativeOutcome, "REJECTED");
    assert.equal(rejected.remoteState?.condition, "amqp:resource-limit-exceeded");
    assert.match(rejected.remoteState.description ?? "", /maximum length/);
  }
);

await test(
  "RabbitMQ publication owns payload/metadata snapshot and rejects oversized bodies before admission",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    let received = false;
    const h = sourceHandler(async (c, _sc, m) => {
      assert.equal(Buffer.from(m.message.body as Buffer).toString(), "original");
      assert.equal(m.message.message_id, "native-id");
      assert.equal(m.message.correlation_id, "native-correlation");
      assert.equal(m.message.application_properties?.["caller"], "kept");
      assert.equal(m.message.message_annotations?.["x-opt-custom"], "annotation");
      assert.equal(c.streamId(), "inherited");
      assert.equal(
        c.metadata().get("traceparent"),
        "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
      );
      await m.accept();
      received = true;
    });
    const source = sourceHarness(url, 9, h, endpoint(9, true, { queueName: q }));
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    const s = sinkHarness(
      url,
      9,
      (c, m, v) => {
        if (v === "oversized") {
          m.message.body = Buffer.alloc(1024 * 1024);
          m.send(c, () => undefined);
          return;
        }
        if (v === "unsupported") {
          m.message.body = { unsupported: true };
          m.send(c, () => undefined);
          return;
        }
        const body = Buffer.from("original");
        const props = { caller: "kept", "X-Stream-ID": "wrong" };
        m.message = {
          body,
          application_properties: props,
          message_id: "native-id",
          correlation_id: "native-correlation",
          message_annotations: { "x-opt-custom": "annotation" }
        };
        m.send(c, () => undefined);
        body.fill(0);
        props.caller = "changed";
        assert.equal(props["X-Stream-ID"], "wrong");
      },
      endpoint(9, false, { queueName: q })
    );
    t.after(() => s.sink.stop(new Context().bounded(1000)));
    await s.sink.start(new Context());
    await s.upstream.emit(new MessageContext(), "oversized");
    assert.match(String(s.errors[0]), /1 MiB/);
    await s.upstream.emit(new MessageContext(), "unsupported");
    assert.match(String(s.errors[1]), /bytes or string/);
    await s.upstream.emit(
      new MessageContext()
        .withMetadata(
          new Map([["traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"]])
        )
        .withStreamId("inherited"),
      "normal"
    );
    await until(() => h.ended.length > 0);
    assert.equal(h.ended[0], undefined);
    assert.ok(received);
  }
);

await test(
  "RabbitMQ source begin failure skips consume/end; consume error reaches exactly one EndRequest without ACK",
  enabled,
  async (t) => {
    const a = await admin(),
      q = name();
    await a.management().declareQueue(q, { type: "quorum" });
    t.after(async () => {
      await a.management().deleteQueue(q);
      await a.close();
    });
    let begins = 0,
      consumes = 0;
    const h = sourceHandler(() => {
      consumes++;
      throw new Error("business failure");
    });
    h.beginRequest = () => {
      begins++;
      throw new Error("begin failure");
    };
    const s = sourceHarness(url, 9, h, endpoint(9, true, { queueName: q }));
    await s.source.start(new Context());
    const p = await a.createPublisher({ queue: { name: q } });
    await p.publish({ body: Buffer.from("one") });
    await until(() => begins === 1);
    await s.source.stop(new Context().bounded(1000));
    assert.equal(consumes, 0);
    assert.equal(h.ended.length, 0);
    h.beginRequest = (c) => ({ context: c, state: undefined });
    const next = sourceHarness(url, 9, h, endpoint(9, true, { queueName: q }));
    await next.source.start(new Context());
    await until(() => h.ended.length === 1);
    assert.equal(h.ended[0]?.message, "business failure");
    await next.source.stop(new Context().bounded(1000));
    const deadline = Date.now() + 2000;
    while ((await a.management().getQueueInfo(q)).getInfo.messageCount !== 1) {
      if (Date.now() > deadline) assert.fail("unsettled message was not redelivered");
      await pause(10);
    }
  }
);

await test("rabbitmq completed requests retain bounded generation cancellation listeners", { ...enabled, timeout: 60000 }, async (t) => {
    const observation = observeCancellationListeners();
    t.after(observation.restore);
    const peer = await admin(), q = name();
    await peer.management().declareQueue(q, { type: "quorum" });
    t.after(async () => { await peer.management().deleteQueue(q); await peer.close(); });
    const publisher = await peer.createPublisher({ queue: { name: q } });
    const handler = sourceHandler(async (_ctx, _sc, m, result) => { await m.accept(); result.done(); });
    const source = sourceHarness(url, 9, handler, endpoint(9, true, { queueName: q }), true);
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    for (let i = 0; i < 1024; i++) {
      await publisher.publish({ body: Buffer.from(String(i)) });
      await until(() => handler.ended.length === i + 1);
      if (i === 127 || i === 1023) observation.assertBounded();
    }
    assert.equal(handler.failures.length, 0);
    await source.source.stop(new Context().bounded(1000));
    observation.assertBounded();
});
