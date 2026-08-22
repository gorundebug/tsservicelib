import assert from "node:assert/strict";
import { test } from "node:test";

import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  stringSerdeType,
  type Completion,
  type Consumer,
  type InputStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

const inputConfig: InputStreamConfig = {
  id: 1,
  name: "processOrder",
  properties: {},
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  valueType: "Order",
  idEndpoint: 100
};

function streamConfig(id: number, name: string, idSource: number): StreamConfig {
  return {
    id,
    name,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource,
    idSources: [],
    xPos: id,
    yPos: 0
  };
}

function environment(configs: readonly StreamConfig[]) {
  return makeTestEnvironment(configs, {
    dataConnectors: [{ id: 10, name: "http", type: 4, properties: {}, implementation: "http" }],
    endpoints: [{ id: 100, name: "processOrder", properties: {}, idDataConnector: 10 }]
  });
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

await test("input routes requests, results and errors through independent outputs", async () => {
  const resultSourceConfig = streamConfig(2, "resultSource", 1);
  const requestSinkConfig = streamConfig(3, "requestSink", 1);
  const errorSinkConfig = streamConfig(4, "errorSink", 1);
  const env = environment([inputConfig, resultSourceConfig, requestSinkConfig, errorSinkConfig]);
  registerTestSerdeType(
    env,
    "Order",
    (value): value is { id: string } =>
      typeof value === "object" && value !== null && "id" in value && typeof value.id === "string"
  );
  const errorType = registerTestSerdeType(
    env,
    "test.Error",
    (value): value is Error => value instanceof Error
  );
  env.serdeRegistry().registerStreamErrorType(inputConfig.id, errorType);
  const input = makeInputStream(inputConfig, env);
  const requestSink = new RecordingStream<{ id: string }>(requestSinkConfig, env);
  const errorSink = new RecordingStream<Error>(errorSinkConfig, env);
  const resultSource = new ConsumedStream(resultSourceConfig, env, env.serde(stringSerdeType));
  const firstResults: string[] = [];
  const secondResults: string[] = [];
  const firstConsumer: Consumer<string> = {
    consume(_context, value): Completion {
      firstResults.push(value);
    }
  };
  const secondConsumer: Consumer<string> = {
    consume(_context, value): Completion {
      secondResults.push(value);
    }
  };

  input.setConsumer(requestSink);
  input.errorStream().setConsumer(errorSink);
  input.setSource(resultSource);
  input.setResultConsumer(firstConsumer);
  await input.consume(new MessageContext(), { id: "order-1" });
  await resultSource.emit(new MessageContext(), "first");
  input.setResultConsumer(secondConsumer);
  await resultSource.emit(new MessageContext(), "second");
  const failure = new Error("failed");
  await input.consumeError(new MessageContext(), failure);

  assert.deepEqual(requestSink.values, [{ id: "order-1" }]);
  assert.deepEqual(firstResults, ["first"]);
  assert.deepEqual(secondResults, ["second"]);
  assert.deepEqual(errorSink.values, [failure]);
  assert.equal(input.endpointId(), 100);
  assert.equal(input.resultStream(), resultSource);
  assert.equal(input.errorStream().id, -input.id);
  assert.equal(env.streamById(input.id), input);
});

await test("input result source can be assigned exactly once", () => {
  const firstConfig = streamConfig(2, "first", 1);
  const secondConfig = streamConfig(3, "second", 1);
  const env = environment([inputConfig, firstConfig, secondConfig]);
  const unknownType = registerTestSerdeType(env, "Order", (value): value is unknown => {
    void value;
    return true;
  });
  env.serdeRegistry().registerStreamErrorType(inputConfig.id, unknownType);
  const input = makeInputStream<unknown, string, unknown>(inputConfig, env);
  const first = new ConsumedStream(firstConfig, env, env.serde(stringSerdeType));
  const second = new ConsumedStream(secondConfig, env, env.serde(stringSerdeType));

  input.setSource(first);
  assert.throws(() => {
    input.setSource(second);
  }, /result source is already set/);
  assert.equal(input.resultStream(), first);
  assert.equal(second.consumer(), undefined);
});
