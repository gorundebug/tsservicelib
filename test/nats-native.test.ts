import assert from "node:assert/strict";
import { test } from "node:test";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { connect, headers } from "@nats-io/transport-node";
import {
  jetstream,
  jetstreamManager,
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  StorageType
} from "@nats-io/jetstream";
import {
  makeNATSEndpointConsumer as makeSource,
  type EndpointHandler as SourceHandler,
  type ConsumerMessage,
  type ResultContext
} from "@gorundebug/tsservicelib/datasource/nats";
import {
  makeNATSEndpointConsumer as makeSink,
  makeNATSEndpointConsumerWithResult as makeSinkWithResult,
  type EndpointHandler as SinkHandler,
  type SinkMessage,
  type Delivery
} from "@gorundebug/tsservicelib/datasink/nats";
import {
  makeInputStream,
  makeSinkStream,
  makeSinkStreamWithResult
} from "@gorundebug/tsservicelib/operators";
import {
  Context,
  MessageContext,
  MessageContextKey,
  ConsumedStream,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type InputStreamConfig,
  type SinkStreamConfig,
  type StreamConfig,
  type NATSDataConnectorConfig,
  type NATSEndpointConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

function present<T>(value: T | undefined | null): T {
  assert.ok(value !== undefined && value !== null);
  return value;
}
const binary = process.env["NATS_SERVER"];
const enabled = { skip: !binary, timeout: 20000 };
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function until(check: () => boolean, timeout = 4000): Promise<void> {
  const end = Date.now() + timeout;
  while (!check()) {
    if (Date.now() > end) throw new Error("condition timed out");
    await pause(10);
  }
}
class Broker {
  public port = 0;
  public child: ChildProcess | undefined;
  public constructor(
    public readonly directory: string,
    private readonly args: readonly string[] = []
  ) {}
  public get url(): string {
    return `nats://127.0.0.1:${String(this.port)}`;
  }
  public async start(): Promise<void> {
    const child = spawn(
      present(binary),
      [
        "-a",
        "127.0.0.1",
        "-p",
        String(this.port || -1),
        "-js",
        "-sd",
        this.directory,
        ...this.args
      ],
      { stdio: ["ignore", "ignore", "pipe"] }
    );
    this.child = child;
    let log = "";
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`broker startup timeout: ${log}`));
      }, 4000);
      child.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.once("exit", (code) => {
        clearTimeout(timer);
        reject(new Error(`broker exit ${String(code)}: ${log}`));
      });
      child.stderr.on("data", (chunk: Buffer) => {
        log += chunk.toString();
        const match = /Listening for client connections on 127\.0\.0\.1:(\d+)/u.exec(log);
        if (match) this.port = Number(match[1]);
        if (log.includes("Server is ready")) {
          clearTimeout(timer);
          resolve();
        }
      });
    });
  }
  public async stop(): Promise<void> {
    const child = this.child;
    if (child?.exitCode !== null) return;
    child.kill("SIGCONT");
    const exited = once(child, "exit");
    child.kill("SIGTERM");
    await exited;
    this.child = undefined;
  }
}
async function broker(): Promise<Broker> {
  const b = new Broker(await mkdtemp(join(tmpdir(), "ts-nats-")));
  await b.start();
  return b;
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
function config(url: string, kind: 7 | 8): NATSDataConnectorConfig {
  return {
    id: 10,
    name: "broker",
    type: kind,
    implementation: "@nats-io/transport-node",
    properties: {},
    servers: [url],
    connectTimeoutMs: 1000,
    operationTimeoutMs: 2000
  };
}
function endpoint(
  kind: 7 | 8,
  source: boolean,
  overrides: Partial<NATSEndpointConfig> = {}
): NATSEndpointConfig {
  return {
    id: source ? 100 : 101,
    name: source ? "receive" : "publish",
    idDataConnector: 10,
    properties: {},
    subject: "events.test",
    enabled: true,
    createStream: false,
    createConsumer: false,
    ...(kind === 8 ? { streamName: "EVENTS", ...(source ? { durableName: "reader" } : {}) } : {}),
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
  kind: 7 | 8,
  handler: SourceHandler<undefined, string, string, Error>,
  ep = endpoint(kind, true),
  results = false,
  connection: Partial<NATSDataConnectorConfig> = {}
) {
  const env = makeTestEnvironment([inputConfig, resultConfig], {
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
  return { env, input, output, record, source: present(env.dataSourceById(10)) };
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
  kind: 7 | 8,
  consume: (c: MessageContext, message: SinkMessage<never>, value: string) => void | Promise<void>,
  ep = endpoint(kind, false),
  metrics = new TestMetrics()
) {
  const upstreamConfig: StreamConfig = { ...inputConfig, type: "Map" };
  const env = makeTestEnvironment([upstreamConfig, sinkConfig], {
    metrics,
    dataConnectors: [config(url, kind)],
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
  return { env, upstream, errors, metrics, sink: present(env.dataSinkById(10)) };
}

await test(
  "native Core bytes, metadata, inherited context, Done is local; graceful drain",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    const key = new MessageContextKey("absent");
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let received = false;
    let inherited = "";
    const handler = sourceHandler(async (c, sc, message, result) => {
      assert.equal(message.jetstream, undefined);
      assert.equal("ack" in message.message, false);
      assert.deepEqual([...message.message.data], [0, 255, 1]);
      assert.deepEqual(message.message.headers?.values("custom"), ["one", "two"]);
      inherited = c.localValue(key);
      received = true;
      await gate;
      await sc.collect(c, "typed");
      result.done();
    });
    const h = sourceHarness(b.url, 7, handler);
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(new MessageContext().withLocalValue(key, "inherited"));
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    const hdr = headers();
    hdr.append("custom", "one");
    hdr.append("custom", "two");
    nc.publish("events.test", new Uint8Array([0, 255, 1]), { headers: hdr });
    await nc.flush();
    await until(() => received);
    let stopped = false;
    const stopping = h.source.stop(Context.background().bounded(1000)).then(() => {
      stopped = true;
    });
    await pause(30);
    assert.equal(stopped, false);
    release();
    await stopping;
    assert.equal(inherited, "inherited");
    assert.deepEqual(handler.ended, [undefined]);
    assert.deepEqual(h.record.values, ["typed"]);
  }
);

for (const kind of [7, 8] as const)
  await test(
    `native ${String(kind)} callback send does not wait for confirmation and drains`,
    enabled,
    async (t) => {
      const b = await broker();
      t.after(async () => {
        await b.stop();
        await rm(b.directory, { recursive: true, force: true });
      });
      const delivered: Delivery[] = [];
      const errors: Error[] = [];
      const metrics = new TestMetrics();
      const labels = {
        protocol: kind === 7 ? "nats" : "jetstream",
        connector: config(b.url, kind).name,
        endpoint: "publish",
        outcome: kind === 7 ? "flushed" : "acknowledged"
      };
      const count = () =>
        metrics.counterValue("broker_endpoint_publication_outcomes_total", labels);
      const h = sinkHarness(
        b.url,
        kind,
        (c, message, value) => {
          message.data = Buffer.from(value);
          message.send(c, (delivery, error) => {
            if (error) errors.push(error);
            else {
              assert.equal(count(), 1);
              delivered.push(present(delivery));
            }
            return undefined;
          });
        },
        endpoint(kind, false, { createStream: kind === 8 }),
        metrics
      );
      t.after(() => h.sink.stop(Context.background().bounded(500)));
      await h.sink.start(Context.background());
      present(b.child).kill("SIGSTOP");
      await h.upstream.emit(new MessageContext(), "bytes");
      assert.deepEqual(h.errors, [undefined]);
      assert.equal(delivered.length, 0);
      assert.equal(count(), 0);
      let drained = false;
      const stopping = h.sink.stop(Context.background().bounded(1500)).then(() => {
        drained = true;
      });
      await pause(30);
      assert.equal(drained, false);
      present(b.child).kill("SIGCONT");
      await stopping;
      assert.equal(delivered.length, 1);
      assert.equal(count(), 1);
      assert.deepEqual(errors, []);
      if (kind === 8) assert.equal(present(delivered[0]).ack?.stream, "EVENTS");
      else assert.equal(present(delivered[0]).ack, undefined);
    }
  );

await test(
  "native JetStream Done without ACK redelivers; explicit ackAck settles",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    const jsm = await jetstreamManager(nc);
    const js = jetstream(nc);
    await jsm.streams.add({ name: "EVENTS", subjects: ["events.*"], storage: StorageType.File });
    await jsm.consumers.add("EVENTS", {
      durable_name: "reader",
      filter_subject: "events.test",
      ack_policy: AckPolicy.Explicit,
      ack_wait: 500_000_000,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant
    });
    let count = 0;
    const handler = sourceHandler(async (_c, _sc, message, result) => {
      count++;
      if (count > 1) assert.equal(await present(message.jetstream).ackAck(), true);
      result.done();
    });
    const h = sourceHarness(b.url, 8, handler, endpoint(8, true), true);
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
    await js.publish("events.test", Buffer.from("unacked"));
    await until(() => handler.ended.length === 1);
    assert.equal((await jsm.consumers.info("EVENTS", "reader")).num_ack_pending, 1);
    await until(() => handler.ended.length >= 2);
    assert.equal((await jsm.consumers.info("EVENTS", "reader")).num_ack_pending, 0);
    assert.deepEqual(handler.ended, [undefined, undefined]);
  }
);

await test(
  "native existing-only startup does not create; explicit creation and incompatible consumers",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    const h = sourceHarness(
      b.url,
      8,
      sourceHandler(() => undefined)
    );
    await assert.rejects(h.source.start(Context.background()), /stream/iu);
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    const jsm = await jetstreamManager(nc);
    await assert.rejects(jsm.streams.info("EVENTS"));
    const created = sourceHarness(
      b.url,
      8,
      sourceHandler((_c, _sc, m) => {
        present(m.jetstream).ack();
      }),
      endpoint(8, true, { createStream: true, createConsumer: true })
    );
    await created.source.start(Context.background());
    await created.source.stop(Context.background().bounded(500));
    const before = await jsm.consumers.info("EVENTS", "reader");
    const invalid = sourceHarness(
      b.url,
      8,
      sourceHandler(() => undefined),
      endpoint(8, true, { subject: "events.other", createStream: true, createConsumer: true })
    );
    await assert.rejects(invalid.source.start(Context.background()));
    assert.equal(
      (await jsm.consumers.info("EVENTS", "reader")).config.filter_subject,
      before.config.filter_subject
    );
  }
);

await test(
  "native Core reconnect resumes subscription after broker restart",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    let count = 0;
    const h = sourceHarness(
      b.url,
      7,
      sourceHandler(() => {
        count++;
      })
    );
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
    let nc = await connect({ servers: b.url, reconnect: false });
    nc.publish("events.test");
    await nc.flush();
    await until(() => count === 1);
    await nc.close();
    await b.stop();
    await b.start();
    await pause(800);
    nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    nc.publish("events.test");
    await nc.flush();
    await until(() => count === 2);
  }
);

await test(
  "native source deadline cancels pending result and ignores late callbacks without ACK",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    let context: MessageContext | undefined;
    let callbacks = 0;
    const handler = sourceHandler((c, _sc, _m, r) => {
      context = c;
      r.setResultCallback("one", () => {
        callbacks++;
        r.done();
        return true;
      });
    });
    const h = sourceHarness(b.url, 7, handler, endpoint(7, true), true);
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    nc.publish("events.test");
    await nc.flush();
    await until(() => !!context);
    await assert.rejects(h.source.stop(Context.background().bounded(30)), /deadline/u);
    await until(() => handler.ended.length === 1);
    assert.ok(handler.ended[0]);
    await h.output.emit(present(context), "late");
    assert.equal(callbacks, 0);
  }
);

await test(
  "native sink saturation is bounded, cancelled-before-admission sends nothing, and Stop wakes pending outcomes",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    const outcomes: (Error | undefined)[] = [];
    const h = sinkHarness(b.url, 7, (c, m) => {
      m.data = new Uint8Array(1);
      m.send(c, (_d, e) => {
        outcomes.push(e);
        return undefined;
      });
    });
    t.after(() => h.sink.stop(Context.background().bounded(500)));
    await h.sink.start(Context.background());
    const cancelled = new AbortController();
    cancelled.abort(new Error("before admission"));
    await assert.rejects(
      Promise.resolve(h.upstream.emit(new MessageContext(cancelled.signal), "no-send")),
      /before admission/u
    );
    present(b.child).kill("SIGSTOP");
    for (let i = 0; i < 257; i++) await h.upstream.emit(new MessageContext(), "queued");
    assert.equal(h.errors.filter(Boolean).length, 1);
    assert.match(present(h.errors[256]).message, /admission limit/u);
    assert.equal(outcomes.length, 0);
    await assert.rejects(h.sink.stop(Context.background().bounded(30)), /deadline/u);
    present(b.child).kill("SIGCONT");
    await until(() => outcomes.length === 256);
    assert.ok(outcomes.every((e) => e instanceof Error));
  }
);

await test(
  "native source overflow reports loss and drains bounded admitted work",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let active = false;
    let count = 0;
    const handler = sourceHandler(async () => {
      count++;
      active = true;
      await gate;
    });
    const h = sourceHarness(b.url, 7, handler);
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    nc.publish("events.test");
    await nc.flush();
    await until(() => active);
    for (let i = 0; i < 300; i++) nc.publish("events.test");
    await nc.flush();
    await until(() => handler.failures.length > 0);
    assert.match(present(handler.failures[0]).message, /overflow/u);
    release();
    await h.source.stop(Context.background().bounded(1000));
    assert.ok(count <= 257);
  }
);

await test("native JS subscriber and sink resume after broker restart", enabled, async (t) => {
  const b = await broker();
  t.after(async () => {
    await b.stop();
    await rm(b.directory, { recursive: true, force: true });
  });
  let count = 0;
  const h = sourceHarness(
    b.url,
    8,
    sourceHandler(async (_c, _sc, m) => {
      await present(m.jetstream).ackAck();
      count++;
    }),
    endpoint(8, true, { createStream: true, createConsumer: true })
  );
  const s = sinkHarness(b.url, 8, async (c, m) => {
    m.data = Buffer.from("value");
    const d = await m.sendSync(c);
    assert.equal(d.ack?.stream, "EVENTS");
  });
  t.after(() => h.source.stop(Context.background().bounded(1000)));
  t.after(() => s.sink.stop(Context.background().bounded(1000)));
  await h.source.start(Context.background());
  await s.sink.start(Context.background());
  await s.upstream.emit(new MessageContext(), "one");
  await until(() => count === 1);
  await b.stop();
  await b.start();
  await pause(900);
  await s.upstream.emit(new MessageContext(), "two");
  await until(() => count === 2);
  assert.deepEqual(s.errors, [undefined, undefined]);
});

await test(
  "native result callback before collect sees pending registration and keeps correlation",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    const handler = sourceHandler(async (c, sc, m, r) => {
      assert.equal(c.streamId(), "correlation");
      assert.equal(
        c.metadata().get("traceparent"),
        "00-11111111111111111111111111111111-2222222222222222-01"
      );
      r.setResultCallback("one", (resultContext, _sc, _state, value) => {
        assert.equal(resultContext.streamId(), "correlation");
        assert.equal(value, "payload");
        r.done();
        return true;
      });
      await sc.collect(c, Buffer.from(m.message.data).toString());
    });
    const h = sourceHarness(b.url, 7, handler, endpoint(7, true), true);
    class Feedback extends ServiceStream implements TypedStreamConsumer<string> {
      public consume(c: MessageContext, v: string) {
        return h.output.emit(c, v);
      }
    }
    h.input.setConsumer(new Feedback(resultConfig, h.env));
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    const hdr = headers();
    hdr.set("X-Stream-ID", "correlation");
    hdr.set("TraceParent", "00-11111111111111111111111111111111-2222222222222222-01");
    nc.publish("events.test", Buffer.from("payload"), { headers: hdr });
    await nc.flush();
    await until(() => handler.ended.length === 1);
    assert.deepEqual(handler.ended, [undefined]);
  }
);

await test(
  "native source begin and consume failures preserve typed lifecycle",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    let begins = 0;
    let consumed = 0;
    const handler = sourceHandler(() => {
      consumed++;
      throw new Error("consume failed");
    });
    handler.beginRequest = (c) => {
      begins++;
      if (begins === 1) throw new Error("begin failed");
      return { context: c, state: undefined };
    };
    const h = sourceHarness(b.url, 7, handler);
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    nc.publish("events.test");
    nc.publish("events.test");
    await nc.flush();
    await until(() => handler.ended.length === 1);
    assert.equal(consumed, 1);
    assert.match(present(handler.ended[0]).message, /consume failed/u);
  }
);

await test("native auth token and verified TLS use SDK configuration", enabled, async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "ts-nats-tls-"));
  const cert = join(dir, "cert.pem"),
    key = join(dir, "key.pem"),
    file = join(dir, "server.conf");
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
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1"
    ],
    { stdio: "ignore" }
  );
  await writeFile(
    file,
    `authorization { token: "integration-test-only" }\ntls { cert_file: "${cert}", key_file: "${key}" }\n`
  );
  const b = new Broker(dir, ["-c", file]);
  t.after(async () => {
    await b.stop();
    await rm(dir, { recursive: true, force: true });
  });
  await b.start();
  const auth = { token: "integration-test-only", tlsEnabled: true, tlsCaFile: cert };
  let received = false;
  const h = sourceHarness(
    b.url,
    7,
    sourceHandler(() => {
      received = true;
    }),
    endpoint(7, true),
    false,
    auth
  );
  t.after(() => h.source.stop(Context.background().bounded(1000)));
  await h.source.start(Context.background());
  const nc = await connect({ servers: b.url, token: auth.token, tls: { caFile: cert } });
  t.after(() => nc.close());
  nc.publish("events.test");
  await nc.flush();
  await until(() => received);
  const bad = sourceHarness(
    b.url,
    7,
    sourceHandler(() => undefined),
    endpoint(7, true),
    false,
    { ...auth, token: "invalid-test-only" }
  );
  await assert.rejects(bad.source.start(Context.background().bounded(1000)));
  const untrusted = sourceHarness(
    b.url,
    7,
    sourceHandler(() => undefined),
    endpoint(7, true),
    false,
    { token: auth.token, tlsEnabled: true }
  );
  await assert.rejects(untrusted.source.start(Context.background().bounded(1000)));
});

await test("native Core queue group distributes rather than broadcasts", enabled, async (t) => {
  const b = await broker();
  t.after(async () => {
    await b.stop();
    await rm(b.directory, { recursive: true, force: true });
  });
  const counts = [0, 0];
  const sources = counts.map((_value, index) =>
    sourceHarness(
      b.url,
      7,
      sourceHandler(() => {
        counts[index] = (counts[index] ?? 0) + 1;
      }),
      endpoint(7, true, { queueGroup: "workers" })
    )
  );
  for (const h of sources) {
    t.after(() => h.source.stop(Context.background().bounded(500)));
    await h.source.start(Context.background());
  }
  const nc = await connect({ servers: b.url });
  t.after(() => nc.close());
  for (let i = 0; i < 40; i++) nc.publish("events.test");
  await nc.flush();
  await until(() => counts.reduce((a, b) => a + b, 0) === 40);
  assert.ok(counts.every((count) => count > 0));
});

await test(
  "native sink typed async/sync/skip results and typed request errors",
  enabled,
  async (t) => {
    const b = await broker();
    t.after(async () => {
      await b.stop();
      await rm(b.directory, { recursive: true, force: true });
    });
    const upstreamConfig: StreamConfig = { ...inputConfig, type: "Map" };
    const metrics = new TestMetrics();
    const env = makeTestEnvironment([upstreamConfig, sinkConfig, resultConfig], {
      metrics,
      dataConnectors: [config(b.url, 8)],
      endpoints: [endpoint(8, false, { createStream: true })]
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
        message.data = Buffer.from(value);
        if (value === "skip") await message.skip(c, "skipped");
        else if (value === "sync") {
          const d = await message.sendSync(c);
          await message.out(c, `sync:${String(d.ack?.seq)}`);
        } else if (value === "error") throw new Error("typed business error");
        else message.send(c, (d, e) => e?.message ?? `async:${String(d?.ack?.seq)}`);
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
    assert.ok(recorded.values.includes("async:1"));
    assert.ok(recorded.values.includes("sync:2"));
    assert.ok(recorded.values.includes("skipped"));
    assert.equal(
      metrics.counterValue("broker_endpoint_publication_outcomes_total", {
        protocol: "jetstream",
        connector: config(b.url, 8).name,
        endpoint: "publish",
        outcome: "acknowledged"
      }),
      2
    );
    assert.equal(errors[0]?.message, "typed business error");
    const nc = await connect({ servers: b.url });
    t.after(() => nc.close());
    const jsm = await jetstreamManager(nc);
    assert.equal((await jsm.streams.info("EVENTS")).state.messages, 2);
  }
);
