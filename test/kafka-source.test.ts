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
  RuntimeConfig,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type InputStreamConfig,
  type KafkaDataConnectorConfig,
  type KafkaEndpointConfig,
  type MessageContext,
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
  makeKafkaEndpointConsumer(input, new Handler(), factory);
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
