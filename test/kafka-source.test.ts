import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type ConsumerMessage,
  type EndpointHandler,
  type KafkaAdmin,
  type KafkaClientFactory,
  type KafkaConsumer,
  type KafkaConsumerControl,
  type KafkaRecord,
  type ResultContext,
  makeKafkaEndpointConsumer
} from "@gorundebug/tsservicelib/datasource/kafka";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  ContextDeadlineExceededError,
  MessageContext,
  RotatingMap,
  RuntimeConfig,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type InputStreamConfig,
  type KafkaDataConnectorConfig,
  type KafkaEndpointConfig,
  type StreamContext,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";

import { makeTestEnvironment, makeTestEnvironmentWithStore } from "./support/environment.js";

const inputConfig: InputStreamConfig = {
  id: 1,
  name: "orderProcessed",
  properties: {},
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 3,
  idSources: [],
  xPos: 0,
  yPos: 0,
  idEndpoint: 100,
  valueType: "string"
};
const resultConfig: StreamConfig = {
  id: 3,
  name: "handled",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0
};
const feedbackConfig: StreamConfig = {
  ...resultConfig,
  id: 4,
  name: "feedback"
};
const connector: KafkaDataConnectorConfig = {
  id: 10,
  name: "events",
  type: 3,
  implementation: "confluent/kafka-javascript",
  properties: {},
  brokers: "redpanda:9092",
  dialTimeout: 250,
  usePartitioner: false,
  async: true,
  securityProtocol: "PLAINTEXT",
  saslMechanism: "PLAIN"
};
const endpoint: KafkaEndpointConfig = {
  id: 100,
  name: "orderProcessed",
  idDataConnector: 10,
  properties: {},
  enabled: true,
  createTopic: true,
  topic: "order-processed",
  partitions: 2,
  consumerGroup: "analytics",
  replicationFactor: 1
};

class FakeAdmin implements KafkaAdmin {
  public readonly topics: string[] = [];
  public connects = 0;
  public connect(): Promise<void> {
    this.connects += 1;
    return Promise.resolve();
  }
  public disconnect(): Promise<void> {
    return Promise.resolve();
  }
  public createTopic(topic: string): Promise<void> {
    this.topics.push(topic);
    return Promise.resolve();
  }
  public partitionCount(): Promise<number> {
    return Promise.resolve(2);
  }
}

class FakeConsumer implements KafkaConsumer {
  #handler: ((record: KafkaRecord, control: KafkaConsumerControl) => Promise<void>) | undefined;
  #resolveRun: (() => void) | undefined;
  #rejectRun: ((error: Error) => void) | undefined;
  public topic = "";
  public connected = false;
  public connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }
  public disconnect(): Promise<void> {
    this.connected = false;
    return Promise.resolve();
  }
  public subscribe(topic: string): Promise<void> {
    this.topic = topic;
    return Promise.resolve();
  }
  public run(
    _concurrency: number,
    handler: (record: KafkaRecord, control: KafkaConsumerControl) => Promise<void>
  ): Promise<void> {
    this.#handler = handler;
    return new Promise((resolve, reject) => {
      this.#resolveRun = resolve;
      this.#rejectRun = reject;
    });
  }
  public stop(): Promise<void> {
    this.#resolveRun?.();
    return Promise.resolve();
  }
  public async feed(record: KafkaRecord, control: KafkaConsumerControl): Promise<void> {
    const handler = this.#handler;
    assert.ok(handler);
    await handler(record, control);
  }
  public fail(error: Error): void {
    this.#rejectRun?.(error);
  }
}

class Factory implements KafkaClientFactory {
  public readonly adminInstance = new FakeAdmin();
  public readonly consumerInstance = new FakeConsumer();
  public consumer(): KafkaConsumer {
    return this.consumerInstance;
  }
  public admin(): KafkaAdmin {
    return this.adminInstance;
  }
}

class ReconnectingFactory implements KafkaClientFactory {
  public readonly adminInstance = new FakeAdmin();
  public readonly consumers: FakeConsumer[] = [];
  public readonly brokerAttempts: string[][] = [];

  public consumer(brokers: readonly string[]): KafkaConsumer {
    this.brokerAttempts.push([...brokers]);
    const consumer = new FakeConsumer();
    this.consumers.push(consumer);
    return consumer;
  }

  public admin(): KafkaAdmin {
    return this.adminInstance;
  }
}

class Control implements KafkaConsumerControl {
  public readonly marked: string[] = [];
  public commits = 0;
  public mark(record: KafkaRecord, metadata: string): void {
    this.marked.push(`${String(record.offset)}:${metadata}`);
  }
  public commit(): Promise<void> {
    this.commits += 1;
    return Promise.resolve();
  }
}

class Handler implements EndpointHandler<undefined, string, string, Error> {
  public readonly transportErrors: Error[] = [];
  public onError(context: MessageContext, stream: StreamContext<string, string, Error>, error: Error): void | Promise<void> {
    this.transportErrors.push(error);
    return stream.errorCollect(context, error);
  }
  public readonly events: string[] = [];
  public beginStreamId: string | undefined;
  public consumeStreamId: string | undefined;
  public concurrency(): number {
    return 1;
  }
  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: undefined }> {
    this.beginStreamId = context.streamId();
    this.events.push("begin");
    return Promise.resolve({ context, state: undefined });
  }
  public consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    _state: undefined,
    message: ConsumerMessage,
    result: ResultContext<undefined, string, string, Error>
  ): Promise<void> {
    this.consumeStreamId = context.streamId();
    const value = Buffer.from(message.value).toString("utf8");
    result.setResultCallback(value, () => {
      message.markMessage("processed");
      result.done();
      return Promise.resolve(true);
    });
    this.events.push(`consume:${value}`);
    return Promise.resolve(stream.collect(context, value));
  }
  public getMessageId(
    _context: MessageContext,
    _stream: StreamContext<string, string, Error>,
    _state: undefined,
    value: string
  ): string {
    return value;
  }
  public endRequest(
    _context: MessageContext,
    _stream: StreamContext<string, string, Error>,
    error: Error | undefined
  ): void {
    this.events.push(error === undefined ? "end" : `end:${error.message}`);
  }
}

class FeedbackStream extends ServiceStream implements TypedStreamConsumer<string> {
  readonly #result: ConsumedStream<string>;

  public constructor(result: ConsumedStream<string>) {
    super(feedbackConfig, result.runtimeEnvironment());
    this.#result = result;
  }

  public consume(context: MessageContext, value: string): Promise<void> {
    return Promise.resolve(this.#result.emit(context, value));
  }
}

await test("Kafka source correlates a pipeline result before marking the offset", async () => {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  const result = new ConsumedStream(resultConfig, environment, environment.serde(stringSerdeType));
  input.setSource(result);
  input.setConsumer(new FeedbackStream(result));
  const factory = new Factory();
  const handler = new Handler();
  makeKafkaEndpointConsumer(input, handler, factory);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);
  await dataSource.start(Context.background());
  const control = new Control();
  await factory.consumerInstance.feed(
    {
      topic: "order-processed",
      partition: 0,
      offset: 7n,
      key: undefined,
      value: Buffer.from("order-1"),
      headers: new Map([["x-stream-id", Buffer.from("transport-invented-id")]])
    },
    control
  );
  assert.deepEqual(handler.events, ["begin", "consume:order-1", "end"]);
  assert.equal(handler.beginStreamId, undefined);
  assert.notEqual(handler.consumeStreamId, "transport-invented-id");
  assert.ok(handler.consumeStreamId);
  assert.deepEqual(control.marked, ["7:processed"]);
  assert.deepEqual(factory.adminInstance.topics, ["order-processed"]);
  await dataSource.stop(Context.background());
  assert.equal(factory.consumerInstance.connected, false);
});

await test("disabled Kafka source remains registered without connecting", async () => {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [{ ...endpoint, enabled: false } as KafkaEndpointConfig]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  const factory = new Factory();
  makeKafkaEndpointConsumer(input, new Handler(), factory);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);
  await dataSource.start(Context.background());
  assert.equal(factory.adminInstance.connects, 0);
  assert.equal(factory.consumerInstance.connected, false);
  await dataSource.stop(Context.background());
});

await test("Kafka source reconnects with the current broker configuration", async () => {
  const { environment, store } = makeTestEnvironmentWithStore([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  const factory = new ReconnectingFactory();
  const handler = new Handler();
  makeKafkaEndpointConsumer(input, handler, factory);
  const dataSource = environment.dataSourceById(10);
  assert.ok(dataSource);
  await dataSource.start(Context.background());
  assert.deepEqual(factory.brokerAttempts, [["redpanda:9092"]]);

  const current = environment.runtimeConfig().config();
  store.publish(
    new RuntimeConfig({
      ...current,
      dataConnectors: [{ ...connector, brokers: "redpanda-a:9092, redpanda-b:9092" }]
    })
  );
  factory.consumers[0]?.fail(new Error("broker transport failed"));
  await waitFor(() => factory.consumers.length === 2);

  assert.deepEqual(handler.transportErrors.map(error => error.message), ["broker transport failed"]);
  assert.deepEqual(handler.events, []);

  assert.deepEqual(factory.brokerAttempts, [
    ["redpanda:9092"],
    ["redpanda-a:9092", "redpanda-b:9092"]
  ]);
  assert.equal(factory.consumers[0]?.connected, false);
  assert.equal(factory.consumers[1]?.connected, true);
  await dataSource.stop(Context.background());
  assert.equal(factory.consumers[1].connected, false);
});

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("condition did not become true");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function makeContextHarness(handler = new Handler()) {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector], endpoints: [endpoint]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  const result = new ConsumedStream(resultConfig, environment, environment.serde(stringSerdeType));
  input.setSource(result);
  input.setConsumer(new FeedbackStream(result));
  const factory = new Factory();
  makeKafkaEndpointConsumer(input, handler, factory);
  const dataSource = environment.dataSourceById(connector.id);
  assert.ok(dataSource);
  const record: KafkaRecord = {
    topic: endpoint.topic, partition: 0, offset: 9n, key: undefined,
    value: Buffer.from("owned-record"), headers: new Map()
  };
  return { dataSource, factory, handler, record };
}

await test("Kafka message and pending-store contexts inherit the source owner", async (t) => {
  const { dataSource, factory, handler, record } = makeContextHarness();
  const rotations = t.mock.method(RotatingMap.prototype, "start");
  const begin = t.mock.method(handler, "beginRequest");
  const cancellation = new AbortController();
  const owner = new MessageContext(cancellation.signal).bounded(60_000).withSampling(true);
  await dataSource.start(owner);
  try {
    assert.equal(rotations.mock.calls[0]?.arguments[0], owner);
    const control = new Control();
    await factory.consumerInstance.feed(record, control);
    const received = begin.mock.calls[0]?.arguments[0];
    assert.ok(received);
    assert.equal(received.deadline(), owner.deadline());
    assert.equal(received.samplingEnabled(), true);
    assert.deepEqual(control.marked, ["9:processed"]);
    const failure = new Error("source owner cancelled");
    cancellation.abort(failure);
    assert.equal(received.cancelled(), true);
    assert.equal(received.signal().reason, failure);
    const next = new Control();
    await assert.rejects(factory.consumerInstance.feed(record, next),
      (error: unknown) => error === failure);
    assert.deepEqual(next.marked, []);
    assert.equal(begin.mock.calls.length, 1);
  } finally {
    await dataSource.stop(new Context());
  }
});

await test("Kafka source can process and mark results after a complete restart", async () => {
  const { dataSource, factory, record } = makeContextHarness();
  for (let run = 0; run < 2; run += 1) {
    const owner = new Context();
    await dataSource.start(owner);
    try {
      const control = new Control();
      await factory.consumerInstance.feed(record, control);
      assert.deepEqual(control.marked, ["9:processed"]);
    } finally {
      await dataSource.stop(owner);
    }
  }
});

await test("Kafka startup rollback forwards owner context and retains failed cleanup", async (t) => {
  const { dataSource } = makeContextHarness();
  const lifecycle = dataSource.endpoint(endpoint.id) as unknown as {
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
  };
  const owner = new Context();
  const retry = new Context();
  const failure = new Error("startup failed");
  const calls: Context[] = [];
  t.mock.method(lifecycle, "start", () => Promise.reject(failure));
  t.mock.method(lifecycle, "stop", (context: Context) => {
    calls.push(context);
    return context === owner ? Promise.reject(new Error("cleanup incomplete")) : Promise.resolve();
  });
  await assert.rejects(dataSource.start(owner), (error: unknown) => error === failure);
  assert.deepEqual(calls, [owner]);
  await assert.rejects(dataSource.start(retry), /already started/);
  await dataSource.stop(retry);
  assert.deepEqual(calls, [owner, retry]);
  await dataSource.stop(retry);
  assert.equal(calls.length, 2);
});

await test("Kafka drain timeout retains an active result callback without marking early", async (t) => {
  const handler = new Handler();
  const entered = Promise.withResolvers<undefined>();
  const release = Promise.withResolvers<undefined>();
  t.mock.method(handler, "consumeMessage", async (
    context: MessageContext, stream: StreamContext<string, string, Error>, _state: undefined,
    message: ConsumerMessage, result: ResultContext<undefined, string, string, Error>
  ) => {
    result.setResultCallback("owned-record", async () => {
      entered.resolve(undefined);
      await release.promise;
      message.markMessage("processed");
      result.done();
      return true;
    });
    await stream.collect(context, "owned-record");
  });
  const { dataSource, factory, record } = makeContextHarness(handler);
  const owner = new Context();
  await dataSource.start(owner);
  const control = new Control();
  const processing = factory.consumerInstance.feed(record, control);
  let retry: Promise<undefined> | undefined;
  try {
    await entered.promise;
    await assert.rejects(dataSource.stop(owner.bounded(5)), { name: "RuntimeDrainTimeoutError" });
    assert.deepEqual(control.marked, []);
    await assert.rejects(dataSource.start(owner), /already started/);
    let stopped = false;
    retry = dataSource.stop(owner).then(() => { stopped = true; return undefined; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(stopped, false);
    release.resolve(undefined);
    await processing;
    await retry;
    assert.deepEqual(control.marked, ["9:processed"]);
    assert.equal(stopped, true);
  } finally {
    release.resolve(undefined);
    await processing;
    await retry;
    await dataSource.stop(owner);
  }
});

await test("Kafka stop retries transport drain with the new caller context", async (t) => {
  const { dataSource, factory } = makeContextHarness();
  const owner = new Context();
  const retry = new Context();
  const failure = new Error("transport drain failed");
  const stop = factory.consumerInstance.stop.bind(factory.consumerInstance);
  const calls: Context[] = [];
  t.mock.method(factory.consumerInstance, "stop", (context: Context) => {
    calls.push(context);
    return calls.length === 1 ? Promise.reject(failure) : stop();
  });
  await dataSource.start(owner);
  await assert.rejects(dataSource.stop(owner), (error: unknown) => error === failure);
  await assert.rejects(dataSource.start(retry), /already started/);
  await dataSource.stop(retry);
  assert.deepEqual(calls, [owner, retry]);
  assert.equal(factory.consumerInstance.connected, false);
});

await test("Kafka transport drain observes cancellation without losing the pending stop", async (t) => {
  const { dataSource, factory } = makeContextHarness();
  const owner = new Context();
  const controller = new AbortController();
  const stoppingContext = new Context(controller.signal);
  const release = Promise.withResolvers<undefined>();
  const entered = Promise.withResolvers<undefined>();
  const stop = factory.consumerInstance.stop.bind(factory.consumerInstance);
  t.mock.method(factory.consumerInstance, "stop", async () => {
    entered.resolve(undefined);
    await release.promise;
    await stop();
  });
  await dataSource.start(owner);
  const reason = new Error("shutdown caller cancelled");
  const stopped = dataSource.stop(stoppingContext);
  const rejected = assert.rejects(stopped, (error: unknown) => error === reason);
  try {
    await entered.promise;
    controller.abort(reason);
    await rejected;
    assert.equal(factory.consumerInstance.connected, true);
    await assert.rejects(dataSource.start(owner), /already started/);
  } finally {
    release.resolve(undefined);
    await dataSource.stop(owner);
  }
  assert.equal(factory.consumerInstance.connected, false);
});

await test("Kafka shutdown distinguishes an early deadline from identically worded cancellation", async (t) => {
  for (const deadline of [true, false]) {
    const { dataSource, factory } = makeContextHarness();
    const owner = new Context();
    const controller = new AbortController();
    const context = new Context(controller.signal).bounded(60_000);
    const reason = deadline
      ? new ContextDeadlineExceededError()
      : new Error("context deadline exceeded");
    const release = Promise.withResolvers<undefined>();
    const stop = factory.consumerInstance.stop.bind(factory.consumerInstance);
    t.mock.method(factory.consumerInstance, "stop", async () => {
      await release.promise;
      await stop();
    });
    await dataSource.start(owner);
    const stopping = dataSource.stop(context);
    const rejected = assert.rejects(stopping, (error: unknown) =>
      deadline
        ? error instanceof Error && error.name === "RuntimeDrainTimeoutError"
        : error === reason
    );
    try {
      controller.abort(reason);
      assert.ok((context.remainingMs() ?? 0) > 0);
      await rejected;
      assert.equal(factory.consumerInstance.connected, true);
    } finally {
      release.resolve(undefined);
      await dataSource.stop(owner);
    }
  }
});

await test("Kafka startup rejects an already cancelled owner without allocating clients", async () => {
  const { dataSource, factory } = makeContextHarness();
  const controller = new AbortController();
  const reason = new Error("startup cancelled before allocation");
  controller.abort(reason);
  await assert.rejects(dataSource.start(new Context(controller.signal)), (error) => error === reason);
  assert.equal(factory.adminInstance.connects, 0);
  assert.equal(factory.consumerInstance.connected, false);
});

for (const phase of ["admin-connect", "metadata", "admin-close", "consumer-connect", "subscribe"] as const) {
  await test(`Kafka startup cancellation retains ${phase} until cleanup finishes`, async (t) => {
    const { dataSource, factory } = makeContextHarness();
    const entered = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const admin = factory.adminInstance;
    const consumer = factory.consumerInstance;
    const gate = async (): Promise<void> => { entered.resolve(undefined); await release.promise; };
    if (phase === "admin-connect") t.mock.method(admin, "connect", gate);
    if (phase === "metadata") t.mock.method(admin, "partitionCount", async () => { await gate(); return 2; });
    if (phase === "admin-close") t.mock.method(admin, "disconnect", gate);
    if (phase === "consumer-connect") {
      const connect = consumer.connect.bind(consumer);
      t.mock.method(consumer, "connect", async () => { await gate(); await connect(); });
    }
    if (phase === "subscribe") {
      const subscribe = consumer.subscribe.bind(consumer);
      t.mock.method(consumer, "subscribe", async (topic: string) => { await gate(); await subscribe(topic); });
    }
    const adminClosed = t.mock.method(admin, "disconnect");
    const consumerClosed = t.mock.method(consumer, "disconnect");
    const runs = t.mock.method(consumer, "run");
    const controller = new AbortController();
    const reason = new Error(`cancel ${phase}`);
    const starting = dataSource.start(new Context(controller.signal));
    const rejected = assert.rejects(starting, (error) => error === reason);
    try {
      await entered.promise;
      controller.abort(reason);
      await rejected;
      await assert.rejects(dataSource.start(new Context()), /already started/);
      await assert.rejects(dataSource.stop(new Context().bounded(5)), { name: "RuntimeDrainTimeoutError" });
      assert.equal(runs.mock.calls.length, 0);
    } finally {
      release.resolve(undefined);
      await dataSource.stop(new Context().bounded(2_000));
    }
    assert.equal(adminClosed.mock.calls.length, 1);
    assert.equal(consumerClosed.mock.calls.length, phase === "consumer-connect" || phase === "subscribe" ? 1 : 0);
    assert.equal(consumer.connected, false);
    assert.equal(runs.mock.calls.length, 0);
    await dataSource.start(new Context());
    await dataSource.stop(new Context());
  });
}

await test("Kafka startup deadline is not reported as a shutdown timeout", async (t) => {
  const { dataSource, factory } = makeContextHarness();
  const release = Promise.withResolvers<undefined>();
  t.mock.method(factory.adminInstance, "connect", () => release.promise);
  try {
    await assert.rejects(dataSource.start(new Context().bounded(5)), ContextDeadlineExceededError);
    await assert.rejects(dataSource.start(new Context()), /already started/);
  } finally {
    release.resolve(undefined);
    await dataSource.stop(new Context().bounded(2_000));
  }
  assert.equal(factory.consumerInstance.connected, false);
});

for (const kind of ["admin", "consumer"] as const) {
  await test(`Kafka failed ${kind} connection preserves primary error and retries failed cleanup`, async (t) => {
    const { dataSource, factory } = makeContextHarness();
    const client = kind === "admin" ? factory.adminInstance : factory.consumerInstance;
    const failure = new Error(`${kind} connect failed`);
    const cleanupFailure = new Error(`${kind} cleanup failed`);
    let allowCleanup = false;
    const disconnect = client.disconnect.bind(client);
    t.mock.method(client, "connect", () => Promise.reject(failure));
    const close = t.mock.method(client, "disconnect", async () => {
      if (!allowCleanup) throw cleanupFailure;
      await disconnect();
    });
    await assert.rejects(dataSource.start(new Context()), (error) => error === failure);
    assert.ok(close.mock.calls.length >= 1);
    await assert.rejects(dataSource.start(new Context()), /already started/);
    allowCleanup = true;
    await dataSource.stop(new Context().bounded(2_000));
    const closed = close.mock.calls.length;
    await dataSource.stop(new Context());
    assert.equal(close.mock.calls.length, closed);
  });
}

await test("Kafka stop during startup prevents a late connection from starting consumption", async (t) => {
  const { dataSource, factory } = makeContextHarness();
  const entered = Promise.withResolvers<undefined>();
  const release = Promise.withResolvers<undefined>();
  const consumer = factory.consumerInstance;
  const connect = consumer.connect.bind(consumer);
  t.mock.method(consumer, "connect", async () => { entered.resolve(undefined); await release.promise; await connect(); });
  const runs = t.mock.method(consumer, "run");
  const starting = dataSource.start(new Context());
  const rejected = assert.rejects(starting, /stopped during startup/);
  try {
    await entered.promise;
    await assert.rejects(dataSource.stop(new Context().bounded(5)), { name: "RuntimeDrainTimeoutError" });
  } finally {
    release.resolve(undefined);
    await rejected;
    await dataSource.stop(new Context().bounded(2_000));
  }
  assert.equal(consumer.connected, false);
  assert.equal(runs.mock.calls.length, 0);
});

await test("Kafka stop retains a pending reconnect without stopping an unconnected consumer", async (t) => {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector], endpoints: [endpoint]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  const factory = new ReconnectingFactory();
  const entered = Promise.withResolvers<undefined>();
  const release = Promise.withResolvers<undefined>();
  const makeConsumer = factory.consumer.bind(factory);
  let prematureStops = 0;
  let lateRuns = 0;
  t.mock.method(factory, "consumer", (brokers: readonly string[]) => {
    const client = makeConsumer(brokers) as FakeConsumer;
    if (factory.consumers.length === 2) {
      const connect = client.connect.bind(client);
      t.mock.method(client, "connect", async () => { entered.resolve(undefined); await release.promise; await connect(); });
      t.mock.method(client, "stop", () => { prematureStops += 1; return Promise.resolve(); });
      t.mock.method(client, "run", () => { lateRuns += 1; return Promise.resolve(); });
    }
    return client;
  });
  makeKafkaEndpointConsumer(input, new Handler(), factory);
  const dataSource = environment.dataSourceById(connector.id);
  assert.ok(dataSource);
  await dataSource.start(new Context());
  factory.consumers[0]?.fail(new Error("reconnect required"));
  try {
    await entered.promise;
    await assert.rejects(dataSource.stop(new Context().bounded(5)), { name: "RuntimeDrainTimeoutError" });
    assert.equal(prematureStops, 0);
  } finally {
    release.resolve(undefined);
    await dataSource.stop(new Context().bounded(2_000));
  }
  assert.equal(lateRuns, 0);
  assert.equal(factory.consumers.length, 2);
  assert.ok(factory.consumers.every((consumer) => !consumer.connected));
});
