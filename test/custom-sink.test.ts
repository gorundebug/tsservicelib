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

await test("custom sink rejects duplicate endpoint binding", () => {
  const harness = makeHarness();
  assert.throws(() => makeCustomEndpointConsumer(harness.sink, harness.handler), /already exists/);
});
