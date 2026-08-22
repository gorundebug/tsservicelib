import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type DataProducer,
  type EndpointHandler,
  type ResultContext,
  makeCustomEndpointConsumer
} from "@gorundebug/tsservicelib/datasource/localsource";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  MessageContext,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type Consumer,
  type DataConnectorConfig,
  type EndpointConfig,
  type InputStreamConfig,
  type StreamConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

const inputConfig: InputStreamConfig = {
  id: 1,
  name: "customInput",
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
  name: "result",
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
const connector: DataConnectorConfig = {
  id: 10,
  name: "custom",
  properties: {},
  type: 4,
  implementation: "custom"
};
const endpoint: EndpointConfig = {
  id: 100,
  name: "customInput",
  properties: {},
  idDataConnector: 10
};

class Producer implements DataProducer<string> {
  #consumer: Consumer<string> | undefined;
  #resolveStop: (() => void) | undefined;
  public started = false;
  public stopped = false;

  public start(_context: Context, consumer: Consumer<string>): Promise<void> {
    this.#consumer = consumer;
    this.started = true;
    return new Promise((resolve) => {
      this.#resolveStop = resolve;
    });
  }

  public stop(): void {
    this.stopped = true;
    this.#resolveStop?.();
  }

  public async emit(context: MessageContext, value: string): Promise<void> {
    const consumer = this.#consumer;
    assert.ok(consumer);
    await consumer.consume(context, value);
  }
}

class Handler implements EndpointHandler<undefined, string, string, Error> {
  public readonly events: string[] = [];

  public concurrency(): number {
    return 1;
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: undefined }> {
    this.events.push("begin");
    return Promise.resolve({ context, state: undefined });
  }

  public consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    _state: undefined,
    value: string,
    result: ResultContext<undefined, string, string, Error>
  ): Promise<void> {
    result.setResultCallback(value, () => {
      this.events.push(`result:${value}`);
      result.done();
      return true;
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

await test("custom source preserves the Go handler lifecycle and result correlation", async () => {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  const result = new ConsumedStream(resultConfig, environment, environment.serde(stringSerdeType));
  input.setSource(result);
  input.setConsumer(new FeedbackStream(result));
  const producer = new Producer();
  const handler = new Handler();
  makeCustomEndpointConsumer(input, producer, handler);
  const dataSource = environment.dataSourceById(connector.id);
  assert.ok(dataSource);

  await dataSource.start(Context.background());
  assert.equal(producer.started, true);
  await producer.emit(new MessageContext(), "order-1");
  assert.deepEqual(handler.events, ["begin", "consume:order-1", "result:order-1", "end"]);

  await dataSource.stop(Context.background());
  assert.equal(producer.stopped, true);
});
