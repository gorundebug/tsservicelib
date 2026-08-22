import assert from "node:assert/strict";
import { test } from "node:test";

import { makeSinkStream, makeSinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  int32SerdeType,
  stringSerdeType,
  type Completion,
  type Consumer,
  type SinkStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

function sourceConfig(id: number): StreamConfig {
  return {
    id,
    name: `source-${String(id)}`,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: 0,
    yPos: 0
  };
}

function sinkConfig(id: number, source: number): SinkStreamConfig {
  return {
    id,
    name: `sink-${String(id)}`,
    properties: {},
    type: "Sink",
    pipeline: "main",
    idService: 1,
    idSource: source,
    idSources: [],
    xPos: 1,
    yPos: 0,
    idEndpoint: 100,
    valueType: "int32"
  };
}

function resultConfig(id: number, source: number): StreamConfig {
  return {
    ...sourceConfig(id),
    name: `result-${String(id)}`,
    type: "Map",
    idSource: source
  };
}

function environment(configs: readonly StreamConfig[]) {
  return makeTestEnvironment(configs, {
    dataConnectors: [{ id: 10, name: "grpc", type: 4, properties: {}, implementation: "grpc" }],
    endpoints: [{ id: 100, name: "remote", properties: {}, idDataConnector: 10 }]
  });
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

await test("terminal sink delegates to a replaceable endpoint consumer", async () => {
  const sourceCfg = sourceConfig(1);
  const sinkCfg = sinkConfig(2, 1);
  const env = environment([sourceCfg, sinkCfg]);
  const errorType = registerTestSerdeType(
    env,
    "test.TerminalError",
    (value): value is Error => value instanceof Error
  );
  env.serdeRegistry().registerStreamErrorType(sinkCfg.id, errorType);
  const source = new ConsumedStream(sourceCfg, env, env.serde(int32SerdeType));
  const sink = makeSinkStream(sinkCfg, source);
  const first: number[] = [];
  const second: number[] = [];
  const firstConsumer: Consumer<number> = {
    consume(_context, value): Completion {
      first.push(value);
    }
  };
  const secondConsumer: Consumer<number> = {
    consume(_context, value): Completion {
      second.push(value);
    }
  };

  await source.emit(new MessageContext(), 0);
  sink.setSinkConsumer(firstConsumer);
  await source.emit(new MessageContext(), 1);
  sink.setSinkConsumer(secondConsumer);
  await source.emit(new MessageContext(), 2);

  assert.deepEqual(first, [1]);
  assert.deepEqual(second, [2]);
  assert.equal(sink.endpointId(), 100);
  assert.equal(sink.errorStream().id, -sink.id);
  assert.equal(env.streamById(sink.id), sink);
});

await test("sink with result has independent result and error outputs", async () => {
  const sourceCfg = sourceConfig(10);
  const sinkCfg = sinkConfig(11, 10);
  const resultCfg = resultConfig(12, 11);
  const errorCfg = resultConfig(13, 11);
  const env = environment([sourceCfg, sinkCfg, resultCfg, errorCfg]);
  const errorType = registerTestSerdeType(
    env,
    "test.ResultError",
    (value): value is Error => value instanceof Error
  );
  env.serdeRegistry().registerStreamErrorType(sinkCfg.id, errorType);
  const source = new ConsumedStream(sourceCfg, env, env.serde(stringSerdeType));
  const sink = makeSinkStreamWithResult(sinkCfg, source);
  const requests: string[] = [];
  const endpoint: Consumer<string> = {
    consume(_context, value): Completion {
      requests.push(value);
    }
  };
  const results = new RecordingStream<number>(resultCfg, env);
  const errors = new RecordingStream<Error>(errorCfg, env);
  sink.setSinkConsumer(endpoint);
  sink.setConsumer(results);
  sink.errorStream().setConsumer(errors);

  await source.emit(new MessageContext(), "request");
  await sink.consumeResult(new MessageContext(), 42);
  const failure = new Error("remote failure");
  await sink.errorStream().emit(new MessageContext(), failure);

  assert.deepEqual(requests, ["request"]);
  assert.deepEqual(results.values, [42]);
  assert.deepEqual(errors.values, [failure]);
});
