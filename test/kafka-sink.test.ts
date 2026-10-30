import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type DeliveryResult,
  type EndpointHandler,
  type KafkaAdmin,
  type KafkaClientFactory,
  type KafkaProducer,
  type Partitioner,
  SinkMessage,
  makeKafkaEndpointConsumer,
  makeKafkaEndpointConsumerWithResult
} from "@gorundebug/tsservicelib/datasink/kafka";
import { makeSinkStream, makeSinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  MessageContext,
  RuntimeConfig,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type KafkaDataConnectorConfig,
  type KafkaEndpointConfig,
  type SinkStreamConfig,
  type Stream,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

import { makeTestEnvironmentWithStore } from "./support/environment.js";

const sourceConfig: StreamConfig = {
  id: 1,
  name: "source",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0
};
const sinkConfig: SinkStreamConfig = {
  id: 2,
  name: "publish",
  properties: {},
  type: "Sink",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0,
  idEndpoint: 100,
  valueType: "error"
};
const resultConfig: StreamConfig = {
  ...sourceConfig,
  id: 3,
  name: "result",
  idSource: 2
};
const connectorConfig: KafkaDataConnectorConfig = {
  id: 10,
  name: "events",
  type: 3,
  implementation: "confluent/kafka-javascript",
  properties: {},
  brokers: "broker-a:9092, broker-b:9092",
  dialTimeout: 500,
  usePartitioner: false,
  async: true,
  securityProtocol: "PLAINTEXT",
  saslMechanism: "PLAIN"
};

function endpointConfig(enabled: boolean): KafkaEndpointConfig {
  return {
    id: 100,
    name: "orderProcessed",
    idDataConnector: 10,
    properties: {},
    enabled,
    createTopic: true,
    topic: "order-processed",
    partitions: 3,
    consumerGroup: "analytics",
    replicationFactor: 1
  };
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];
  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

class FakeProducer implements KafkaProducer {
  public readonly sent: {
    readonly topic: string;
    readonly key?: Uint8Array;
    readonly value: Uint8Array;
    readonly partition?: number;
    readonly headers?: ReadonlyMap<string, string>;
  }[] = [];
  public connected = false;
  public flushed = false;
  public deferDelivery = false;
  public nextFailure: Error | undefined;
  #completeDelivery: (() => void) | undefined;
  public connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }
  public disconnect(): Promise<void> {
    this.connected = false;
    return Promise.resolve();
  }
  public flush(): Promise<void> {
    this.flushed = true;
    return Promise.resolve();
  }
  public send(
    topic: string,
    key: Uint8Array | undefined,
    value: Uint8Array,
    partition?: number,
    headers?: ReadonlyMap<string, string>
  ): Promise<DeliveryResult> {
    this.sent.push({
      topic,
      ...(key === undefined ? {} : { key }),
      value,
      ...(partition === undefined ? {} : { partition }),
      ...(headers === undefined ? {} : { headers })
    });
    if (this.nextFailure !== undefined) {
      const failure = this.nextFailure;
      this.nextFailure = undefined;
      return Promise.reject(failure);
    }
    if (this.deferDelivery) {
      return new Promise((resolve) => {
        this.#completeDelivery = () => {
          resolve({ partition: 2, offset: 41n });
        };
      });
    }
    return Promise.resolve({ partition: 2, offset: 41n });
  }

  public completeDelivery(): void {
    const complete = this.#completeDelivery;
    assert.ok(complete, "a deferred Kafka delivery must be pending");
    this.#completeDelivery = undefined;
    complete();
  }
}

class FakeAdmin implements KafkaAdmin {
  public readonly topics: string[] = [];
  public connect(): Promise<void> {
    return Promise.resolve();
  }
  public disconnect(): Promise<void> {
    return Promise.resolve();
  }
  public createTopic(topic: string, partitions: number, replicationFactor: number): Promise<void> {
    this.topics.push(`${topic}:${String(partitions)}:${String(replicationFactor)}`);
    return Promise.resolve();
  }
  public partitionCount(): Promise<number> {
    return Promise.resolve(3);
  }
}

class FakeFactory implements KafkaClientFactory {
  public producerCreations = 0;
  public readonly producerInstance = new FakeProducer();
  public readonly adminInstance = new FakeAdmin();
  public readonly brokers: string[][] = [];
  public producer(brokers: readonly string[]): KafkaProducer {
    this.brokers.push([...brokers]);
    this.producerCreations++;
    return this.producerInstance;
  }
  public admin(brokers: readonly string[]): KafkaAdmin {
    this.brokers.push([...brokers]);
    return this.adminInstance;
  }
}

class ReconnectingFactory implements KafkaClientFactory {
  public readonly adminInstance = new FakeAdmin();
  public readonly producers: FakeProducer[] = [];
  public readonly producerBrokers: string[][] = [];

  public producer(brokers: readonly string[]): KafkaProducer {
    this.producerBrokers.push([...brokers]);
    const producer = new FakeProducer();
    this.producers.push(producer);
    return producer;
  }

  public admin(): KafkaAdmin {
    return this.adminInstance;
  }
}

interface State {
  readonly orderId: string;
}

class Handler implements EndpointHandler<State, string, Error>, Partitioner<string> {
  public readonly events: string[] = [];
  public getStreamId(_context: MessageContext, value: string): string {
    return value;
  }
  public partition(_value: string, partitions: number): number {
    return partitions - 1;
  }
  public beginRequest(
    context: MessageContext,
    _stream: Stream
  ): Promise<{ readonly context: MessageContext; readonly state: State }> {
    void _stream;
    this.events.push("begin");
    return Promise.resolve({ context, state: { orderId: "order-1" } });
  }
  public consumeMessage(
    context: MessageContext,
    _stream: Stream,
    state: State,
    value: string,
    message: SinkMessage<Error>
  ): void {
    this.events.push(`consume:${state.orderId}`);
    message.key = Buffer.from(state.orderId);
    message.value = Buffer.from(value);
    message.send(context, (_partition, _offset, error) => error);
  }
  public endRequest(_context: MessageContext, _stream: Stream, error: Error | undefined): void {
    this.events.push(error === undefined ? "end" : `end:${error.message}`);
  }
}

function makeHarness(enabled: boolean, usePartitioner = false, tracing?: TestTracing) {
  return makeHarnessWithFactory(new FakeFactory(), enabled, usePartitioner, tracing);
}

function makeHarnessWithFactory<TFactory extends KafkaClientFactory>(
  factory: TFactory,
  enabled: boolean,
  usePartitioner = false,
  tracing?: TestTracing
) {
  const configuredConnector: KafkaDataConnectorConfig = {
    ...connectorConfig,
    usePartitioner
  };
  const { environment, store } = makeTestEnvironmentWithStore(
    [sourceConfig, sinkConfig, resultConfig],
    {
      dataConnectors: [configuredConnector],
      endpoints: [endpointConfig(enabled)],
      ...(tracing === undefined ? {} : { tracing })
    }
  );
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  const sink = makeSinkStream(sinkConfig, source);
  const results = new RecordingStream<Error>(resultConfig, environment);
  sink.errorStream().setConsumer(results);
  const handler = new Handler();
  makeKafkaEndpointConsumer(sink, handler, factory);
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink);
  return { source, results, factory, handler, dataSink, store };
}

await test("Kafka sink creates its topic, publishes asynchronously and drains delivery", async () => {
  const harness = makeHarness(true);
  harness.factory.producerInstance.deferDelivery = true;
  await harness.dataSink.start(Context.background());
  await harness.source.emit(new MessageContext(), "payload");
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(harness.factory.brokers[0], ["broker-a:9092", "broker-b:9092"]);
  assert.deepEqual(harness.factory.adminInstance.topics, ["order-processed:3:1"]);
  assert.equal(harness.factory.producerInstance.sent[0]?.topic, "order-processed");
  assert.deepEqual(harness.results.values, []);
  assert.deepEqual(harness.handler.events, ["begin", "consume:order-1", "end"]);
  let stopped = false;
  const stopping = harness.dataSink.stop(Context.background()).then(() => {
    stopped = true;
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(stopped, false, "data sink stop must drain accepted Kafka delivery callbacks");
  harness.factory.producerInstance.completeDelivery();
  await stopping;
  assert.equal(harness.factory.producerInstance.flushed, true);
});

await test("Kafka sink records the canonical Go transport events", async () => {
  const tracing = new TestTracing();
  const harness = makeHarness(true, false, tracing);
  await harness.dataSink.start(Context.background());
  await harness.source.emit(
    new MessageContext()
      .withMetadata(
        new Map([
          ["traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"],
          ["unrelated", "must-not-cross-transport"]
        ])
      )
      .withSampling(true),
    "payload"
  );
  const headers = harness.factory.producerInstance.sent[0]?.headers;
  assert.ok(headers);
  assert.equal(headers.get("x-trace"), "1");
  assert.equal(
    headers.get("traceparent"),
    "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
  );
  assert.equal(headers.has("unrelated"), false);
  const span = tracing.spans().find(({ name }) => name === "kafka.output");
  assert.ok(span);
  assert.deepEqual(
    span.events.map(({ name }) => name),
    ["begin_request", "consume_message"]
  );
  await harness.dataSink.stop(Context.background());
});

await test("Kafka synchronous delivery observes an already-cancelled context", async () => {
  let sends = 0;
  const message = new SinkMessage<Error>(
    "order-processed",
    () => {
      sends += 1;
    },
    () => undefined
  );
  const cancellation = new AbortController();
  const failure = new Error("delivery cancelled");
  cancellation.abort(failure);

  await assert.rejects(message.sendSync(new MessageContext(cancellation.signal)), failure);
  assert.equal(sends, 1, "Go SendSync submits before observing context cancellation");
});

await test("disabled Kafka sink remains present without connecting or publishing", async () => {
  const harness = makeHarness(false);
  await harness.dataSink.start(Context.background());
  await harness.source.emit(new MessageContext(), "payload");
  assert.deepEqual(harness.factory.brokers, []);
  assert.deepEqual(harness.factory.producerInstance.sent, []);
  assert.deepEqual(harness.handler.events, []);
  await harness.dataSink.stop(Context.background());
});

await test("Kafka sink applies the configured handler partitioner", async () => {
  const harness = makeHarness(true, true);
  await harness.dataSink.start(Context.background());
  await harness.source.emit(new MessageContext(), "payload");
  assert.equal(harness.factory.producerInstance.sent[0]?.partition, 2);
  await harness.dataSink.stop(Context.background());
});

await test("Kafka sink reconnects future deliveries with the current brokers", async () => {
  const factory = new ReconnectingFactory();
  const harness = makeHarnessWithFactory(factory, true);
  await harness.dataSink.start(Context.background());
  const first = factory.producers[0];
  assert.ok(first);

  const current = harness.source.runtimeEnvironment().runtimeConfig().config();
  harness.store.publish(
    new RuntimeConfig({
      ...current,
      dataConnectors: [
        { ...connectorConfig, brokers: "broker-c:9092,broker-d:9092", usePartitioner: false }
      ]
    })
  );
  first.nextFailure = new Error("delivery failed after broker loss");
  await harness.source.emit(new MessageContext(), "failed-payload");
  await waitFor(() => harness.results.values.length === 1);

  assert.deepEqual(factory.producerBrokers, [
    ["broker-a:9092", "broker-b:9092"],
    ["broker-c:9092", "broker-d:9092"]
  ]);
  assert.equal(first.connected, false);
  assert.equal(factory.producers[1]?.connected, true);
  assert.match(harness.results.values[0]?.message ?? "", /delivery failed/);

  await harness.source.emit(new MessageContext(), "recovered-payload");
  await waitFor(() => (factory.producers[1]?.sent.length ?? 0) === 1);
  const second = factory.producers[1];
  assert.ok(second);
  assert.equal(Buffer.from(second.sent[0]?.value ?? []).toString(), "recovered-payload");
  await harness.dataSink.stop(Context.background());
  assert.equal(second.connected, false);
});

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("condition did not become true");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function makeResultHarness(mode: "skip" | "async" | "sync") {
  const configured = { ...sinkConfig, valueType: "string" };
  const { environment } = makeTestEnvironmentWithStore([sourceConfig, configured, resultConfig], {
    dataConnectors: [connectorConfig],
    endpoints: [endpointConfig(true)]
  });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  environment.serdeRegistry().registerStreamErrorType(configured.id, errorSerdeType);
  const sink = makeSinkStreamWithResult<string, string, Error>(configured, source);
  const results = new RecordingStream<string>(resultConfig, environment);
  const errors = new RecordingStream<Error>(
    { ...resultConfig, id: 4, name: "errors" },
    environment
  );
  sink.setConsumer(results);
  sink.errorStream().setConsumer(errors);
  const ended: (Error | undefined)[] = [];
  const handler: EndpointHandler<undefined, string, string> = {
    getStreamId: (_context, value) => value,
    beginRequest: (context) => ({ context, state: undefined }),
    async consumeMessage(context, _stream, _state, value, message) {
      message.value = Buffer.from(value);
      if (mode === "skip") await message.skip(context, "skipped");
      else if (mode === "sync") {
        const result = await message.sendSync(context);
        await message.out(context, String(result.offset));
      } else {
        message.send(context, (_partition, offset, error) => error?.message ?? String(offset));
      }
    },
    endRequest: (_context, _stream, error) => {
      ended.push(error);
    }
  };
  const factory = new FakeFactory();
  makeKafkaEndpointConsumerWithResult(sink, handler, factory);
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink);
  return { environment, source, sink, results, errors, ended, factory, dataSink };
}

for (const mode of ["skip", "async", "sync"] as const) {
  await test(`Kafka SinkWithResult ${mode} uses ordinary continuation after completion`, async () => {
    const h = makeResultHarness(mode);
    const context = new MessageContext();
    h.factory.producerInstance.deferDelivery = true;
    await h.dataSink.start(context);
    try {
      const pending = h.source.emit(context, "request-1");
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (mode !== "skip") {
        assert.deepEqual(h.results.values, []);
        h.factory.producerInstance.completeDelivery();
      }
      await pending;
      await h.dataSink.stop(context);
      assert.deepEqual(h.results.values, [mode === "skip" ? "skipped" : "41"]);
      assert.deepEqual(h.errors.values, []);
      assert.deepEqual(h.ended, [undefined]);
    } finally {
      await h.dataSink.stop(context);
    }
  });
}

await test("Kafka shared endpoint keeps ordinary and result-bearing bindings independent", async () => {
  const h = makeResultHarness("async");
  const plainConfig = { ...sinkConfig, id: 5, name: "plain" };
  const plainSource = new ConsumedStream(
    sourceConfig,
    h.environment,
    h.environment.serde(stringSerdeType)
  );
  h.environment.serdeRegistry().registerStreamErrorType(plainConfig.id, errorSerdeType);
  const plain = makeSinkStream<string, Error>(plainConfig, plainSource);
  const failures = new RecordingStream<Error>(
    { ...resultConfig, id: 6, name: "plain-errors" },
    h.environment
  );
  plain.errorStream().setConsumer(failures);
  makeKafkaEndpointConsumer(plain, new Handler(), h.factory);
  const context = new MessageContext();
  await h.dataSink.start(context);
  assert.equal(h.factory.producerCreations, 1, "both bindings start with one producer");
  const failure = new Error("producer rejected");
  try {
    h.factory.producerInstance.nextFailure = failure;
    await plainSource.emit(context, "plain");
    await new Promise<void>((resolve) => setImmediate(resolve));
    await h.source.emit(context, "with-result");
    await h.dataSink.stop(context);
    assert.deepEqual(failures.values, [failure]);
    assert.deepEqual(h.results.values, ["41"]);
    assert.deepEqual(h.errors.values, []);
    assert.equal(
      h.factory.producerCreations,
      2,
      "one replacement after the injected broker failure"
    );
  } finally {
    await h.dataSink.stop(context);
  }
});

await test("Kafka SinkWithResult cancelled synchronous wait cannot emit late success", async () => {
  const h = makeResultHarness("sync");
  const cancellation = new AbortController();
  const context = new MessageContext(cancellation.signal);
  h.factory.producerInstance.deferDelivery = true;
  await h.dataSink.start(context);
  const pending = h.source.emit(context, "cancelled");
  await new Promise<void>((resolve) => setImmediate(resolve));
  cancellation.abort(new Error("request cancelled"));
  await pending;
  h.factory.producerInstance.completeDelivery();
  await h.dataSink.stop(new Context());
  assert.deepEqual(h.results.values, []);
  assert.deepEqual(h.errors.values, []);
  assert.equal(h.ended[0]?.message, "request cancelled");
});
