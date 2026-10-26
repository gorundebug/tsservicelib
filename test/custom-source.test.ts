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

function makeLifecycleHarness(producer: DataProducer<string> = new Producer()) {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint]
  });
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<string, string, Error>(inputConfig, environment);
  makeCustomEndpointConsumer(input, producer, new Handler());
  const dataSource = environment.dataSourceById(connector.id);
  assert.ok(dataSource);
  const lifecycle = dataSource.endpoint(endpoint.id) as unknown as {
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
  };
  return { environment, dataSource, lifecycle };
}

await test("custom producer failure is logged with its start owner context", async (t) => {
  const failure = new Error("producer failure");
  const observed = Promise.withResolvers<Context>();
  const harness = makeLifecycleHarness({
    start: () => Promise.reject(failure),
    stop: () => undefined
  });
  t.mock.method(harness.environment.log(), "error", (context: Context) => {
    observed.resolve(context);
  });
  const owner = new MessageContext().withStreamId("producer-owner");
  await harness.dataSource.start(owner);
  assert.equal(await observed.promise, owner);
  await harness.dataSource.stop(owner);
});

await test("custom source rollback preserves context and failed cleanup remains retryable", async (t) => {
  const { dataSource, lifecycle } = makeLifecycleHarness();
  const owner = new MessageContext().withStreamId("startup-owner");
  const retry = new MessageContext().withStreamId("shutdown-owner");
  const failure = new Error("startup failure");
  const stops: Context[] = [];
  t.mock.method(lifecycle, "start", () => Promise.reject(failure));
  t.mock.method(lifecycle, "stop", (context: Context) => {
    stops.push(context);
    return context === owner ? Promise.reject(new Error("cleanup incomplete")) : Promise.resolve();
  });
  await assert.rejects(dataSource.start(owner), (error: unknown) => error === failure);
  assert.deepEqual(stops, [owner]);
  await assert.rejects(dataSource.start(retry), /already started/);
  await dataSource.stop(retry);
  assert.deepEqual(stops, [owner, retry]);
  await dataSource.stop(retry);
  assert.equal(stops.length, 2);
});

await test("custom producer stop failure keeps its endpoint available for retry", async () => {
  const contexts: Context[] = [];
  const harness = makeLifecycleHarness({
    start: () => undefined,
    stop(context): void {
      contexts.push(context);
      if (contexts.length === 1) throw new Error("stop incomplete");
    }
  });
  const owner = new MessageContext();
  const retry = new MessageContext();
  await harness.dataSource.start(owner);
  await assert.rejects(harness.dataSource.stop(owner), /stop incomplete/);
  await assert.rejects(harness.dataSource.start(retry), /already started/);
  await harness.dataSource.stop(retry);
  assert.deepEqual(contexts, [owner, retry]);
});

await test("custom source processes correlated replies after a complete stop and restart", async () => {
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

  for (const value of ["first-run", "second-run"]) {
    const owner = new MessageContext().withStreamId(value);
    await dataSource.start(owner);
    try {
      await producer.emit(owner, value);
    } finally {
      await dataSource.stop(owner);
    }
  }
  assert.deepEqual(handler.events, [
    "begin", "consume:first-run", "result:first-run", "end",
    "begin", "consume:second-run", "result:second-run", "end"
  ]);
});

await test("custom source drain timeout preserves the running producer until retry completes", async () => {
  const running = Promise.withResolvers<undefined>();
  const harness = makeLifecycleHarness({
    start: () => running.promise,
    stop: () => undefined
  });
  const owner = new MessageContext();
  await harness.dataSource.start(owner);
  let retry: Promise<undefined> | undefined;
  try {
    await assert.rejects(harness.dataSource.stop(owner.bounded(5)), { name: "RuntimeDrainTimeoutError" });
    await assert.rejects(harness.dataSource.start(owner), /already started/);
    let completed = false;
    retry = harness.dataSource.stop(owner).then(() => {
      completed = true;
      return undefined;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(completed, false);
    running.resolve(undefined);
    await retry;
    assert.equal(completed, true);
    await harness.dataSource.stop(owner);
  } finally {
    running.resolve(undefined);
    await retry;
    await harness.dataSource.stop(owner);
  }
});
