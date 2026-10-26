import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type EndpointHandler,
  type SinkCallback,
  makeCustomEndpointConsumer
} from "@gorundebug/tsservicelib/datasink/localsink";
import { makeSinkStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  DataConnectorType,
  MessageContext,
  ServiceStream,
  errorSerdeType,
  stringSerdeType,
  type Collector,
  type CustomDataConnectorConfig,
  type CustomEndpointConfig,
  type SinkStreamConfig,
  type Stream,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";

import { makeTestEnvironment } from "./support/environment.js";

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
  name: "customSink",
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
const connectorConfig: CustomDataConnectorConfig = {
  id: 10,
  name: "custom",
  type: DataConnectorType.Custom,
  implementation: "custom",
  properties: {}
};
const endpointConfig: CustomEndpointConfig = {
  id: 100,
  name: "customSink",
  idDataConnector: 10,
  properties: {}
};

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

interface State {
  readonly prefix: string;
}

class Handler implements EndpointHandler<State, string, Error> {
  public readonly events: string[] = [];
  public fail = false;

  public getStreamId(_context: MessageContext, value: string): string {
    return `stream-${value}`;
  }

  public beginRequest(
    context: MessageContext,
    _stream: Stream
  ): Promise<{ readonly context: MessageContext; readonly state: State }> {
    void _stream;
    this.events.push("begin");
    return Promise.resolve({ context, state: { prefix: "result" } });
  }

  public async consumeMessage(
    context: MessageContext,
    _stream: Stream,
    state: State,
    value: string,
    resultStream: Collector<Error>
  ): Promise<void> {
    this.events.push(`consume:${context.streamId() ?? ""}:${value}`);
    if (this.fail) throw new Error("sink failure");
    await resultStream.out(context, new Error(`${state.prefix}-1`));
    await resultStream.out(context, new Error(`${state.prefix}-2`));
  }

  public endRequest(
    _context: MessageContext,
    _stream: Stream,
    error: Error | undefined,
    _state: State
  ): void {
    void _state;
    this.events.push(error === undefined ? "end" : `end:${error.message}`);
  }
}

function makeHarness() {
  const environment = makeTestEnvironment([sourceConfig, sinkConfig, resultConfig], {
    dataConnectors: [connectorConfig],
    endpoints: [endpointConfig]
  });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  const sink = makeSinkStream(sinkConfig, source);
  const results = new RecordingStream<Error>(resultConfig, environment);
  sink.errorStream().setConsumer(results);
  const handler = new Handler();
  makeCustomEndpointConsumer(sink, handler);
  const dataSink = environment.dataSinkById(connectorConfig.id);
  assert.ok(dataSink);
  const callbackConsumer = dataSink.endpoint(endpointConfig.id)?.endpointConsumers()[0] as
    { setSinkCallback(callback: SinkCallback<string>): void } | undefined;
  assert.ok(callbackConsumer);
  return { source, sink, results, handler, dataSink, callbackConsumer };
}

await test("custom sink preserves lifecycle, multi-push and completion callback", async () => {
  const harness = makeHarness();
  const completions: string[] = [];
  harness.callbackConsumer.setSinkCallback({
    done(context, value, error): void {
      completions.push(`${context.streamId() ?? ""}:${value}:${error?.message ?? "ok"}`);
    }
  });

  await harness.dataSink.start(Context.background());
  await harness.source.emit(new MessageContext().withStreamId("original"), "value");
  await harness.dataSink.stop(Context.background());

  assert.deepEqual(harness.handler.events, ["begin", "consume:stream-value:value", "end"]);
  assert.deepEqual(
    harness.results.values.map((value) => value.message),
    ["result-1", "result-2"]
  );
  assert.deepEqual(completions, ["original:value:ok"]);
});

await test("custom sink reports handler failure to EndRequest and callback", async () => {
  const harness = makeHarness();
  harness.handler.fail = true;
  let callbackFailure: Error | undefined;
  harness.callbackConsumer.setSinkCallback({
    done(_context, _value, error): void {
      callbackFailure = error;
    }
  });

  await harness.dataSink.start(Context.background());
  await harness.source.emit(new MessageContext(), "value");
  await harness.dataSink.stop(Context.background());

  assert.deepEqual(harness.handler.events, [
    "begin",
    "consume:stream-value:value",
    "end:sink failure"
  ]);
  assert.equal(callbackFailure?.message, "sink failure");
  assert.deepEqual(harness.results.values, []);
});

await test("custom sink shares one endpoint between independent sink streams", async () => {
  const secondSinkConfig: SinkStreamConfig = {
    ...sinkConfig,
    id: 4,
    name: "secondCustomSink",
    idSource: 5
  };
  const secondSourceConfig: StreamConfig = {
    ...sourceConfig,
    id: 5,
    name: "secondSource"
  };
  const environment = makeTestEnvironment(
    [sourceConfig, sinkConfig, resultConfig, secondSinkConfig, secondSourceConfig],
    { dataConnectors: [connectorConfig], endpoints: [endpointConfig] }
  );
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  environment.serdeRegistry().registerStreamErrorType(secondSinkConfig.id, errorSerdeType);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  const secondSource = new ConsumedStream(
    secondSourceConfig,
    environment,
    environment.serde(stringSerdeType)
  );
  const first = makeSinkStream(sinkConfig, source);
  const second = makeSinkStream(secondSinkConfig, secondSource);
  const firstHandler = new Handler();
  const secondHandler = new Handler();

  makeCustomEndpointConsumer(first, firstHandler);
  makeCustomEndpointConsumer(second, secondHandler);

  const dataSink = environment.dataSinkById(connectorConfig.id);
  assert.ok(dataSink);
  const endpoint = dataSink.endpoint(endpointConfig.id);
  assert.ok(endpoint);
  assert.equal(dataSink.endpoints().length, 1);
  assert.equal(endpoint.endpointConsumers().length, 2);

  await dataSink.start(Context.background());
  await first.consume(new MessageContext(), "first");
  await second.consume(new MessageContext(), "second");
  await dataSink.stop(Context.background());

  assert.deepEqual(firstHandler.events, ["begin", "consume:stream-first:first", "end"]);
  assert.deepEqual(secondHandler.events, ["begin", "consume:stream-second:second", "end"]);
});

await test("custom sink rollback preserves context and retains failed cleanup for retry", async (t) => {
  const { dataSink } = makeHarness();
  const lifecycle = dataSink.endpoint(endpointConfig.id) as unknown as {
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
  };
  const owner = new MessageContext().withStreamId("startup-owner");
  const retry = new MessageContext().withStreamId("shutdown-owner");
  const failure = new Error("startup failure");
  const stops: Context[] = [];
  t.mock.method(lifecycle, "start", () => Promise.reject(failure));
  t.mock.method(lifecycle, "stop", (context: Context) => {
    stops.push(context);
    return context === owner ? Promise.reject(new Error("cleanup incomplete")) : Promise.resolve();
  });
  await assert.rejects(dataSink.start(owner), (error: unknown) => error === failure);
  assert.deepEqual(stops, [owner]);
  await assert.rejects(dataSink.start(retry), /already started/);
  await dataSink.stop(retry);
  assert.deepEqual(stops, [owner, retry]);
  await dataSink.stop(retry);
  assert.equal(stops.length, 2);
});

await test("custom sink failed stop is retryable with a new caller context", async (t) => {
  const { dataSink } = makeHarness();
  const lifecycle = dataSink.endpoint(endpointConfig.id) as unknown as {
    stop(context: Context): Promise<void>;
  };
  const owner = new MessageContext();
  const retry = new MessageContext();
  const stops: Context[] = [];
  t.mock.method(lifecycle, "stop", (context: Context) => {
    stops.push(context);
    return stops.length === 1 ? Promise.reject(new Error("stop incomplete")) : Promise.resolve();
  });
  await dataSink.start(owner);
  await assert.rejects(dataSink.stop(owner), /stop incomplete/);
  await assert.rejects(dataSink.start(retry), /already started/);
  await dataSink.stop(retry);
  assert.deepEqual(stops, [owner, retry]);
});
