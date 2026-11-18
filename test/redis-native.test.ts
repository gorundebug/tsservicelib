import { observeCancellationListeners } from "./support/cancellation-listeners.js";
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { createServer, connect, type Socket } from "node:net";
import { createServer as createTLSServer } from "node:tls";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "redis";
import {
  makeRedisStreamsEndpointConsumer as makeSource,
  type EndpointHandler as SourceHandler,
  type ConsumerMessage,
  type ResultContext
} from "@gorundebug/tsservicelib/datasource/redisstreams";
import {
  makeRedisStreamsEndpointConsumer as makeSink,
  makeRedisStreamsEndpointConsumerWithResult as makeSinkWithResult,
  type EndpointHandler as SinkHandler,
  type SinkMessage,
  type Delivery
} from "@gorundebug/tsservicelib/datasink/redisstreams";
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
  type RedisStreamsDataConnectorConfig,
  type RedisStreamsEndpointConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
function present<T>(v: T | undefined | null): T {
  assert.ok(v !== undefined && v !== null);
  return v;
}
const enabled = { skip: process.env["REDIS_NATIVE"] !== "1", timeout: 20000 };
const pause = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
async function until(check: () => boolean | Promise<boolean>, timeout = 5000): Promise<void> {
  const end = Date.now() + timeout;
  while (!(await check())) {
    if (Date.now() > end) throw new Error("condition timed out");
    await pause(10);
  }
}
const sharedURL = process.env["REDIS_TEST_URL"];
let url = sharedURL ?? "";
const ownedKeys = new Set<string>();
let container = "";
function name(): string {
  const key = "ts-stream-" + randomUUID();
  ownedKeys.add(key);
  return key;
}
async function admin(database = 0) {
  const c = createClient({
    url,
    password: "adapter-test",
    database,
    RESP: 2,
    socket: { reconnectStrategy: false }
  });
  c.on("error", () => undefined);
  await c.connect();
  return c;
}
before(async () => {
  if (enabled.skip) return;
  if (sharedURL) {
    const c = await admin();
    c.destroy();
    return;
  }
  container = "tsservicelib-redis-streams-" + randomUUID();
  execFileSync(
    "docker",
    [
      "run",
      "--rm",
      "--detach",
      "--name",
      container,
      "-p",
      "127.0.0.1::6379",
      "redis:7.4-alpine",
      "redis-server",
      "--save",
      "",
      "--appendonly",
      "no",
      "--requirepass",
      "adapter-test"
    ],
    { stdio: "pipe" }
  );
  const binding = execFileSync("docker", ["port", container, "6379/tcp"], {
    encoding: "utf8"
  }).trim();
  url = "redis://" + binding;
  await until(async () => {
    try {
      const c = await admin();
      c.destroy();
      return true;
    } catch {
      return false;
    }
  }, 10000);
});
after(async () => {
  if (!enabled.skip && sharedURL) {
    for (const database of [0, 1]) {
      const c = await admin(database);
      try {
        if (ownedKeys.size) await c.del([...ownedKeys]);
      } finally {
        c.destroy();
      }
    }
  }
  if (container) execFileSync("docker", ["rm", "--force", container], { stdio: "pipe" });
});
async function setup(key: string, group = "workers") {
  const c = await admin();
  await c.sendCommand(["XGROUP", "CREATE", key, group, "0-0", "MKSTREAM"]);
  return c;
}
async function pending(
  c: Awaited<ReturnType<typeof admin>>,
  key: string,
  group = "workers"
): Promise<number> {
  const v = await c.sendCommand<unknown[]>(["XPENDING", key, group]);
  return v[0] as number;
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
function config(url: string, kind: 10): RedisStreamsDataConnectorConfig {
  return {
    id: 10,
    name: "broker",
    type: kind,
    implementation: "node-redis",
    properties: {},
    servers: [url],
    redisDatabase: 0,
    password: "adapter-test",
    connectTimeoutMs: 1000,
    operationTimeoutMs: 2000
  };
}
function endpoint(
  kind: 10,
  source: boolean,
  overrides: Partial<RedisStreamsEndpointConfig> = {}
): RedisStreamsEndpointConfig {
  return {
    id: source ? 100 : 101,
    name: source ? "receive" : "publish",
    idDataConnector: 10,
    properties: {},
    streamKey: "unused",
    enabled: true,
    ...(source
      ? { consumerGroup: "workers", readBlockMs: 50, claimIdleMs: 100, claimIntervalMs: 50 }
      : { createStream: false }),
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
  kind: 10,
  handler: SourceHandler<undefined, string, string, Error>,
  ep = endpoint(kind, true),
  results = false,
  connection: Partial<RedisStreamsDataConnectorConfig> = {}
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
  return { metrics, count: (observation: string) => metrics.counterValue("broker_endpoint_source_observations_total", {protocol: "redisstreams", connector: config(url, kind).name, endpoint: ep.name, observation}), env, input, output, record, source: present(env.dataSourceById(10)) };
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
  kind: 10,
  consume: (c: MessageContext, message: SinkMessage<never>, value: string) => void | Promise<void>,
  ep = endpoint(kind, false),
  connection: Partial<RedisStreamsDataConnectorConfig> = {}
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

void test(
  "Redis Streams native binary fields/duplicates, metadata and explicit XACK versus Done",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    let held: ConsumerMessage | undefined;
    const received: ConsumerMessage[] = [];
    const h = sourceHandler((ctx, _sc, m, r) => {
      held = m;
      received.push(m);
      assert.equal(ctx.streamId(), "inherited");
      r.done();
    });
    const source = sourceHarness(
      url,
      10,
      h,
      endpoint(10, true, { streamKey: key, claimIdleMs: 60000 }),
      true
    );
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    const outcomes: Delivery[] = [];
    const sink = sinkHarness(
      url,
      10,
      (ctx, m) => {
        const bytes = Buffer.from([0, 255, 1]);
        m.fields = [
          ["duplicate", "one"],
          ["duplicate", "two"],
          [Buffer.from([255]), bytes],
          ["caller", "retained"],
          ["X-Stream-ID", "wrong"]
        ];
        m.send(ctx, (d) => {
          outcomes.push(present(d));
        });
        bytes.fill(4);
      },
      endpoint(10, false, { streamKey: key })
    );
    t.after(() => sink.sink.stop(new Context().bounded(1000)));
    await sink.sink.start(new Context());
    await sink.upstream.emit(new MessageContext().withStreamId("inherited"), "");
    await until(() => h.ended.length === 1 && outcomes.length === 1);
    assert.equal(outcomes[0]?.outcome, "published");
    assert.equal(outcomes[0].id, received[0]?.message.id);
    const fields = present(received[0]).message.fields;
    assert.deepEqual(
      fields.filter(([k]) => k.toString() === "duplicate").map(([, v]) => v.toString()),
      ["one", "two"]
    );
    assert.deepEqual(
      fields.find(([k]) => k.equals(Buffer.from([255])))?.[1],
      Buffer.from([0, 255, 1])
    );
    assert.equal(await pending(c, key), 1);
    await assert.rejects(() => present(held).ack(), /expired/);
    assert.equal(await c.xLen(key), 1);
    await source.source.stop(new Context().bounded(1000));
    let ackCount = -1;
    const second = sourceHarness(
      url,
      10,
      sourceHandler(async (_ctx, _sc, m, r) => {
        ackCount = await m.ack();
        r.done();
      }),
      endpoint(10, true, { streamKey: key, claimIdleMs: 1 }),
      true
    );
    t.after(() => second.source.stop(new Context().bounded(1000)));
    await second.source.start(new Context());
    await until(() => ackCount === 1);
    assert.equal(await pending(c, key), 0);
    assert.equal(await c.xLen(key), 1);
  }
);

void test(
  "Redis Streams explicit creation, NOMKSTREAM nil and WRONGTYPE are confirmed rejections",
  enabled,
  async (t) => {
    const key = name(),
      c = await admin();
    t.after(() => {
      c.destroy();
    });
    const outcomes: Delivery[] = [];
    const noCreate = sinkHarness(
      url,
      10,
      async (ctx, m) => {
        m.fields = [["body", "value"]];
        outcomes.push(await m.sendSync(ctx));
      },
      endpoint(10, false, { streamKey: key })
    );
    t.after(() => noCreate.sink.stop(new Context().bounded(1000)));
    await noCreate.sink.start(new Context());
    await noCreate.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[0]?.outcome, "rejected");
    assert.equal(await c.exists(key), 0);
    const create = sinkHarness(
      url,
      10,
      async (ctx, m) => {
        m.fields = [["body", "created"]];
        outcomes.push(await m.sendSync(ctx));
      },
      endpoint(10, false, { streamKey: key, createStream: true })
    );
    t.after(() => create.sink.stop(new Context().bounded(1000)));
    await create.sink.start(new Context());
    await create.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[1]?.outcome, "published");
    assert.equal(await c.xLen(key), 1);
    await c.del(key);
    await c.set(key, "wrong");
    await noCreate.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[2]?.outcome, "rejected");
    assert.match(outcomes[2].error?.message ?? "", /WRONGTYPE/);
    const absent = sourceHarness(
      url,
      10,
      sourceHandler(() => undefined),
      endpoint(10, true, { streamKey: name() })
    );
    await assert.rejects(() => absent.source.start(new Context().bounded(1000)));
  }
);

void test(
  "Redis Streams opt-in group positions never reset existing group; callback registered before collect",
  enabled,
  async (t) => {
    const key = name(),
      c = await admin();
    t.after(() => {
      c.destroy();
    });
    await c.xAdd(key, "*", { body: "old" });
    let seen = 0;
    const h = sourceHandler(async (ctx, sc, m, r) => {
      seen++;
      r.setResultCallback("one", async () => {
        await m.ack();
        r.done();
        return true;
      });
      await sc.collect(ctx, "value");
    });
    const source = sourceHarness(
      url,
      10,
      h,
      endpoint(10, true, { streamKey: key, createConsumerGroup: true, groupStartId: "$" }),
      true
    );
    class Results extends ServiceStream implements TypedStreamConsumer<string> {
      public consume(ctx: MessageContext, v: string) {
        return source.output.emit(ctx, v);
      }
    }
    source.input.setConsumer(new Results(resultConfig, source.env));
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    await pause(100);
    assert.equal(seen, 0);
    await c.xAdd(key, "*", { body: "new" });
    await until(() => h.ended.length === 1);
    assert.equal(await pending(c, key), 0);
    await source.source.stop(new Context().bounded(1000));
    const next = sourceHarness(
      url,
      10,
      sourceHandler(async (_ctx, _sc, m) => {
        seen++;
        await m.ack();
      }),
      endpoint(10, true, { streamKey: key, createConsumerGroup: true, groupStartId: "0-0" })
    );
    t.after(() => next.source.stop(new Context().bounded(1000)));
    await next.source.start(new Context());
    await pause(150);
    assert.equal(seen, 1);
    await c.xAdd(key, "*", { body: "later" });
    await until(() => seen === 2);
  }
);

void test(
  "Redis Streams reclaim advances empty pages and reports deleted PEL entries",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    const ids: string[] = [];
    for (let i = 0; i < 12; i++) ids.push(await c.xAdd(key, "*", { body: String(i) }));
    await c.sendCommand([
      "XREADGROUP",
      "GROUP",
      "workers",
      "abandoned",
      "COUNT",
      "12",
      "STREAMS",
      key,
      ">"
    ]);
    const old = present(ids[10]),
      deleted = present(ids[11]);
    await c.sendCommand([
      "XCLAIM",
      key,
      "workers",
      "abandoned",
      "0",
      old,
      deleted,
      "IDLE",
      "200000"
    ]);
    await c.xDel(key, deleted);
    const got: string[] = [];
    const h = sourceHandler(async (_ctx, _sc, m) => {
      got.push(m.message.id);
      assert.ok(m.message.reclaimed);
      await m.ack();
    });
    const source = sourceHarness(
      url,
      10,
      h,
      endpoint(10, true, {
        streamKey: key,
        readCount: 1,
        claimIdleMs: 100000,
        claimIntervalMs: 20,
        readBlockMs: 20
      })
    );
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    await until(
      () => got.includes(old) && h.failures.some((e) => e.message.includes("deleted entries"))
    );
    assert.equal(await pending(c, key), 10);
    assert.deepEqual(got, [old]);
    assert.equal(source.count("received"), 1);
    assert.equal(source.count("reclaimed"), 1);
  }
);

void test(
  "Redis Streams dedicated read connection, bounded active handlers, graceful ACK drain",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let active = 0,
      peak = 0,
      acks = 0;
    const h = sourceHandler(async (_ctx, _sc, m) => {
      active++;
      peak = Math.max(peak, active);
      await gate;
      const start = Date.now();
      await m.ack();
      assert.ok(Date.now() - start < 500);
      acks++;
      active--;
    });
    const source = sourceHarness(
      url,
      10,
      h,
      endpoint(10, true, {
        streamKey: key,
        readCount: 2,
        readBlockMs: 60000,
        claimIntervalMs: 60000,
        claimIdleMs: 60000
      })
    );
    await source.source.start(new Context());
    for (let i = 0; i < 6; i++) await c.xAdd(key, "*", { body: String(i) });
    await until(() => active === 2);
    assert.equal(peak, 2);
    assert.equal(await pending(c, key), 2);
    const stopped = source.source.stop(new Context().bounded(1500));
    await pause(30);
    assert.equal(h.ended.length, 0);
    release();
    await stopped;
    assert.equal(acks, 2);
    assert.equal(await pending(c, key), 0);
    assert.equal(await c.xLen(key), 6);
  }
);

void test(
  "Redis Streams async callbacks outlive EndRequest and retain bounded admission",
  enabled,
  async () => {
    const key = name();
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let callbacks = 0;
    const s = sinkHarness(
      url,
      10,
      (ctx, m) => {
        m.fields = [["body", "value"]];
        m.send(ctx, async (d) => {
          assert.equal(d?.outcome, "published");
          callbacks++;
          await gate;
        });
      },
      endpoint(10, false, { streamKey: key, createStream: true, publisherMaxInFlight: 1 })
    );
    await s.sink.start(new Context());
    await s.upstream.emit(new MessageContext(), "one");
    await until(() => callbacks === 1);
    assert.deepEqual(s.errors, [undefined]);
    await s.upstream.emit(new MessageContext(), "two");
    assert.match(String(s.errors[1]), /admission/);
    const start = Date.now();
    await assert.rejects(() => s.sink.stop(new Context().bounded(60)));
    assert.ok(Date.now() - start < 500);
    release();
  }
);

void test(
  "Redis Streams source Stop bounds cancellation-resistant handler and leaves PEL pending",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let entered = false;
    let message: ConsumerMessage | undefined;
    const h = sourceHandler(async (_ctx, _sc, m) => {
      entered = true;
      message = m;
      await gate;
    });
    const s = sourceHarness(url, 10, h, endpoint(10, true, { streamKey: key, readCount: 1 }));
    await s.source.start(new Context());
    await c.xAdd(key, "*", { body: "pending" });
    await until(() => entered);
    const start = Date.now();
    await assert.rejects(() => s.source.stop(new Context().bounded(60)));
    assert.ok(Date.now() - start < 500);
    assert.equal(h.ended.length, 0);
    await assert.rejects(() => present(message).ack(), /expired/);
    release();
    await until(() => h.ended.length === 1);
    assert.equal(await pending(c, key), 1);
  }
);

async function proxy(tls?: { key: Buffer; cert: Buffer }) {
  const sockets = new Set<Socket>();
  let blockReplies = false;
  const bridge = (front: Socket): void => {
    const upstream = new URL(url),
      back = connect({ host: upstream.hostname, port: Number(upstream.port || 6379) });
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
    url: `${tls ? "rediss://localhost" : "redis://127.0.0.1"}:${String(address.port)}`,
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
void test(
  "Redis Streams lost XADD reply stays unknown, never replayed, later publication recovers",
  enabled,
  async (t) => {
    const bridge = await proxy(),
      key = name(),
      c = await admin();
    t.after(async () => {
      await bridge.close();
      c.destroy();
    });
    const outcomes: Delivery[] = [];
    const sink = sinkHarness(
      bridge.url,
      10,
      (ctx, m) => {
        m.fields = [["body", "value"]];
        m.send(ctx, (d) => {
          outcomes.push(present(d));
        });
      },
      endpoint(10, false, { streamKey: key, createStream: true }),
      { operationTimeoutMs: 150 }
    );
    t.after(() => sink.sink.stop(new Context().bounded(1000)));
    await sink.sink.start(new Context());
    bridge.block();
    await sink.upstream.emit(new MessageContext(), "one");
    await until(() => outcomes.length === 1);
    assert.equal(outcomes[0]?.outcome, "unknown");
    assert.equal(await c.xLen(key), 1);
    bridge.unblock();
    await pause(400);
    await sink.upstream.emit(new MessageContext(), "two");
    await until(() => outcomes.length === 2);
    assert.equal(outcomes[1]?.outcome, "published");
    assert.equal(await c.xLen(key), 2);
  }
);

void test(
  "Redis Streams connection interruption fences ACK handles and reclaims lost work",
  enabled,
  async (t) => {
    const bridge = await proxy(),
      key = name(),
      c = await setup(key);
    t.after(async () => {
      await bridge.close();
      c.destroy();
    });
    let old: ConsumerMessage | undefined;
    let count = 0;
    const h = sourceHandler(async (_ctx, _sc, m, r) => {
      count++;
      if (count === 1) old = m;
      else {
        assert.ok(m.message.reclaimed);
        await m.ack();
        r.done();
      }
    });
    const s = sourceHarness(
      bridge.url,
      10,
      h,
      endpoint(10, true, { streamKey: key, claimIdleMs: 100, readCount: 1 }),
      true
    );
    t.after(() => s.source.stop(new Context().bounded(1000)));
    await s.source.start(new Context());
    await c.xAdd(key, "*", { body: "pending" });
    await until(() => count === 1);
    bridge.drop();
    await until(() => count === 2 && h.ended.length === 2);
    await assert.rejects(() => present(old).ack(), /expired/);
    assert.equal(await pending(c, key), 0);
    assert.ok(h.ended[0] instanceof Error);
  }
);

void test(
  "Redis Streams independent consumers have unique identities and share work",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    const counts = [0, 0];
    const consumerNames: string[] = [];
    for (let i = 0; i < 2; i++) {
      const h = sourceHandler(async (_ctx, _sc, m) => {
        counts[i] = (counts[i] ?? 0) + 1;
        await m.ack();
      });
      const s = sourceHarness(
        url,
        10,
        h,
        endpoint(10, true, { streamKey: key, readCount: 1, claimIdleMs: 60000 })
      );
      t.after(() => s.source.stop(new Context().bounded(1000)));
      await s.source.start(new Context());
    }
    for (let i = 0; i < 10; i++) await c.xAdd(key, "*", { body: String(i) });
    await until(() => counts.reduce((a, b) => a + b, 0) === 10);
    assert.ok(counts.every((n) => n > 0));
    const raw = await c.sendCommand<unknown[][]>(["XINFO", "CONSUMERS", key, "workers"]);
    for (const row of raw) {
      const ix = row.indexOf("name");
      consumerNames.push(String(row[ix + 1]));
    }
    assert.equal(new Set(consumerNames).size, 2);
    assert.equal(await pending(c, key), 0);
  }
);

void test(
  "Redis Streams pre-admission cancellation differs from unknown post-transmission outcome",
  enabled,
  async (t) => {
    const bridge = await proxy(),
      key = name(),
      c = await admin();
    t.after(async () => {
      await bridge.close();
      c.destroy();
    });
    const outcomes: Delivery[] = [];
    const sink = sinkHarness(
      bridge.url,
      10,
      (ctx, m) => {
        m.fields = [["body", "value"]];
        m.send(ctx, (d) => {
          outcomes.push(present(d));
        });
      },
      endpoint(10, false, { streamKey: key, createStream: true })
    );
    t.after(() => sink.sink.stop(new Context().bounded(1000)));
    await sink.sink.start(new Context());
    const before = new AbortController();
    before.abort();
    await assert.rejects(async () => sink.upstream.emit(new MessageContext(before.signal), "none"));
    assert.equal(await c.exists(key), 0);
    assert.equal(outcomes.length, 0);
    const after = new AbortController();
    bridge.block();
    await sink.upstream.emit(new MessageContext(after.signal), "maybe");
    await until(async () => (await c.xLen(key)) === 1);
    after.abort();
    await until(() => outcomes.length === 1);
    assert.equal(outcomes[0]?.outcome, "unknown");
  }
);

void test(
  "Redis Streams auth, database selection and startup timeout use native connection settings",
  enabled,
  async (t) => {
    const bridge = await proxy();
    t.after(() => bridge.close());
    bridge.block();
    const stalled = sinkHarness(
      bridge.url,
      10,
      () => undefined,
      endpoint(10, false, { streamKey: name() }),
      { connectTimeoutMs: 100 }
    );
    const started = Date.now();
    await assert.rejects(() => stalled.sink.start(new Context()), /startup failed|deadline/);
    assert.ok(Date.now() - started < 500);
    const bad = sinkHarness(url, 10, () => undefined, endpoint(10, false, { streamKey: name() }), {
      password: "SECRET"
    });
    await assert.rejects(
      () => bad.sink.start(new Context()),
      (e) => e instanceof Error && !e.message.includes("SECRET")
    );
    const key = name(),
      c0 = await admin(),
      c1 = await admin(1);
    t.after(() => {
      c0.destroy();
      c1.destroy();
    });
    const outcomes: Delivery[] = [];
    const good = sinkHarness(
      url,
      10,
      async (ctx, m) => {
        m.fields = [["body", "db1"]];
        outcomes.push(await m.sendSync(ctx));
      },
      endpoint(10, false, { streamKey: key, createStream: true }),
      { redisDatabase: 1 }
    );
    t.after(() => good.sink.stop(new Context().bounded(1000)));
    await good.sink.start(new Context());
    await good.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[0]?.outcome, "published");
    assert.equal(await c0.exists(key), 0);
    assert.equal(await c1.xLen(key), 1);
  }
);

void test(
  "Redis Streams TLS verification supports CA and hostname override; untrusted peer fails",
  enabled,
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "ts-redis-tls-")),
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
        "/CN=redis-test",
        "-addext",
        "subjectAltName=DNS:redis-test"
      ],
      { stdio: "ignore" }
    );
    const bridge = await proxy({ key: await readFile(key), cert: await readFile(cert) });
    t.after(async () => {
      await bridge.close();
      await rm(directory, { recursive: true, force: true });
    });
    const outcomes: Delivery[] = [];
    const good = sinkHarness(
      bridge.url,
      10,
      async (ctx, m) => {
        m.fields = [["body", "tls"]];
        outcomes.push(await m.sendSync(ctx));
      },
      endpoint(10, false, { streamKey: name(), createStream: true }),
      { tlsCaFile: cert, tlsServerName: "redis-test" }
    );
    t.after(() => good.sink.stop(new Context().bounded(1000)));
    await good.sink.start(new Context());
    await good.upstream.emit(new MessageContext(), "");
    assert.equal(outcomes[0]?.outcome, "published");
    const bad = sinkHarness(
      bridge.url,
      10,
      () => undefined,
      endpoint(10, false, { streamKey: name() }),
      { connectTimeoutMs: 200 }
    );
    await assert.rejects(() => bad.sink.start(new Context()));
  }
);

void test(
  "Redis Streams typed async/sync/skip outcomes preserve R and E routes",
  enabled,
  async (t) => {
    const key = name(),
      upstreamConfig: StreamConfig = { ...inputConfig, type: "Map" };
    const env = makeTestEnvironment([upstreamConfig, sinkConfig, resultConfig], {
      dataConnectors: [config(url, 10)],
      endpoints: [endpoint(10, false, { streamKey: key, createStream: true })]
    });
    env.serdeRegistry().registerStreamErrorType(2, errorSerdeType);
    const upstream = new ConsumedStream(upstreamConfig, env, env.serde(stringSerdeType)),
      output = makeSinkStreamWithResult<string, string, Error>(sinkConfig, upstream),
      record = new Recording(resultConfig, env);
    output.setConsumer(record);
    const errors: Error[] = [];
    class Errors extends ServiceStream implements TypedStreamConsumer<Error> {
      public consume(_ctx: MessageContext, error: Error) {
        errors.push(error);
      }
    }
    output.errorStream().setConsumer(new Errors({ ...resultConfig, id: 4, name: "errors" }, env));
    const h: SinkHandler<undefined, string, string, Error> = {
      getStreamId: () => "correlation",
      beginRequest: (ctx) => ({ context: ctx, state: undefined }),
      consumeMessage: async (ctx, _sc, _s, value, m) => {
        m.fields = [["body", value]];
        if (value === "skip") await m.skip(ctx, "skipped");
        else if (value === "error") throw new Error("business error");
        else if (value === "sync") {
          const d = await m.sendSync(ctx);
          await m.out(ctx, `${d.outcome}:${d.id ?? ""}`);
        } else m.send(ctx, (d) => `${d?.outcome ?? ""}:${d?.id ?? ""}`);
      },
      endRequest: (ctx, sc, error) => {
        if (error) return sc.errorCollect(ctx, error);
      }
    };
    makeSinkWithResult(output, h);
    const sink = present(env.dataSinkById(10));
    t.after(() => sink.stop(new Context().bounded(1000)));
    await sink.start(new Context());
    for (const value of ["async", "sync", "skip", "error"])
      await upstream.emit(new MessageContext(), value);
    await sink.stop(new Context().bounded(1000));
    assert.equal(record.values.filter((v) => v.startsWith("published:")).length, 2);
    assert.ok(record.values.includes("skipped"));
    assert.equal(errors[0]?.message, "business error");
  }
);

void test(
  "Redis Streams source begin/consume errors do not ACK or invent graph errors",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    let begins = 0,
      consumed = 0;
    const h = sourceHandler(() => {
      consumed++;
      throw new Error("consume failure");
    });
    h.beginRequest = () => {
      begins++;
      throw new Error("begin failure");
    };
    const first = sourceHarness(
      url,
      10,
      h,
      endpoint(10, true, { streamKey: key, readCount: 1, claimIdleMs: 60000 })
    );
    await first.source.start(new Context());
    await c.xAdd(key, "*", { body: "pending" });
    await until(() => begins === 1);
    await first.source.stop(new Context().bounded(1000));
    assert.equal(consumed, 0);
    assert.equal(h.ended.length, 0);
    h.beginRequest = (ctx) => ({ context: ctx, state: undefined });
    const second = sourceHarness(
      url,
      10,
      h,
      endpoint(10, true, { streamKey: key, claimIdleMs: 1 })
    );
    t.after(() => second.source.stop(new Context().bounded(1000)));
    await second.source.start(new Context());
    await until(() => h.ended.length > 0);
    await second.source.stop(new Context().bounded(1000));
    assert.equal(h.ended[0]?.message, "consume failure");
    assert.equal(await pending(c, key), 1);
  }
);

void test(
  "Redis Streams rejects an actual replica before admitting work",
  { ...enabled, skip: enabled.skip || !!sharedURL },
  async () => {
    const c = await admin();
    try {
      await c.sendCommand(["REPLICAOF", "127.0.0.1", "1"]);
      const s = sinkHarness(url, 10, () => undefined, endpoint(10, false, { streamKey: name() }));
      await assert.rejects(() => s.sink.start(new Context().bounded(1000)), /primary/);
    } finally {
      await c.sendCommand(["REPLICAOF", "NO", "ONE"]);
      c.destroy();
    }
  }
);
void test(
  "Redis Streams oversized native entries remain pending; oversized sends never publish",
  enabled,
  async (t) => {
    const key = name(),
      c = await setup(key);
    t.after(() => {
      c.destroy();
    });
    let consumed = 0;
    const h = sourceHandler(() => {
      consumed++;
    });
    const s = sourceHarness(url, 10, h, endpoint(10, true, { streamKey: key, claimIdleMs: 60000 }));
    t.after(() => s.source.stop(new Context().bounded(1000)));
    await s.source.start(new Context());
    await c.xAdd(key, "*", { body: "x".repeat(1024 * 1024) });
    await until(() => h.failures.length > 0);
    assert.equal(consumed, 0);
    assert.equal(await pending(c, key), 1);
    const sink = sinkHarness(
      url,
      10,
      async (ctx, m) => {
        m.fields = [["body", Buffer.alloc(1024 * 1024)]];
        await assert.rejects(() => m.sendSync(ctx), /1 MiB/);
      },
      endpoint(10, false, { streamKey: key })
    );
    t.after(() => sink.sink.stop(new Context().bounded(1000)));
    await sink.sink.start(new Context());
    await sink.upstream.emit(new MessageContext(), "");
    assert.equal(await c.xLen(key), 1);
  }
);

await test("redis completed requests retain bounded generation cancellation listeners", { ...enabled, timeout: 60000 }, async (t) => {
    const observation = observeCancellationListeners();
    t.after(observation.restore);
    const key = name(), peer = await setup(key);
    t.after(() => peer.destroy());
    const handler = sourceHandler(async (_ctx, _sc, m, result) => { await m.ack(); result.done(); });
    const source = sourceHarness(url, 10, handler, endpoint(10, true, { streamKey: key, claimIdleMs: 60000 }), true);
    t.after(() => source.source.stop(new Context().bounded(1000)));
    await source.source.start(new Context());
    for (let i = 0; i < 1024; i++) {
      await peer.xAdd(key, "*", { value: String(i) });
      await until(() => handler.ended.length === i + 1);
      if (i === 127 || i === 1023) observation.assertBounded();
    }
    assert.equal(handler.failures.length, 0);
    await source.source.stop(new Context().bounded(1000));
    observation.assertBounded();
});
