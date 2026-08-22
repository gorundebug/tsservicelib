import assert from "node:assert/strict";
import { test } from "node:test";

import {
  any,
  bool,
  ConsumedStream,
  err,
  float64,
  FunctionCollector,
  int,
  int64,
  makeSinkStreamContext,
  makeStreamContext,
  MessageContext,
  str,
  type Context,
  type LogField,
  type Logger,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

class RecordingLogger implements Logger {
  public readonly fields: LogField[] = [];

  public debug(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    void message;
    this.fields.push(...fields);
  }

  public info(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.debug(context, message, ...fields);
  }

  public warn(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.debug(context, message, ...fields);
  }

  public error(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.debug(context, message, ...fields);
  }
}

function streamConfig(id: number, name: string): StreamConfig {
  return {
    id,
    name,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: id,
    yPos: 0
  };
}

await test("source and sink contexts preserve typed collectors and the service logger", async () => {
  const logger = new RecordingLogger();
  const sourceConfig = streamConfig(1, "source");
  const resultConfig = streamConfig(2, "result");
  const environment = makeTestEnvironment([sourceConfig, resultConfig], { logger });
  const source = new ConsumedStream(sourceConfig, environment, makeTestSerde<string>());
  const result = new ConsumedStream(resultConfig, environment, makeTestSerde<number>());
  const context = new MessageContext();
  const sourceValues: string[] = [];
  const sourceErrors: Error[] = [];
  const sinkResults: number[] = [];
  const sinkErrors: Error[] = [];
  const sourceContext = makeStreamContext(
    source,
    result,
    new FunctionCollector((_context, value: string) => {
      sourceValues.push(value);
    }),
    new FunctionCollector((_context, value: Error) => {
      sourceErrors.push(value);
    })
  );
  const sinkContext = makeSinkStreamContext<string, number, Error>(
    result,
    new FunctionCollector((_context, value: number) => {
      sinkResults.push(value);
    }),
    new FunctionCollector((_context, value: Error) => {
      sinkErrors.push(value);
    })
  );
  const failure = new Error("failed");

  await sourceContext.collect(context, "request");
  await sourceContext.errorCollect(context, failure);
  await sinkContext.collect(context, 42);
  await sinkContext.errorCollect(context, failure);

  assert.equal(sourceContext.stream, source);
  assert.equal(sourceContext.resultStream, result);
  assert.equal(sourceContext.logger, logger);
  assert.equal(sinkContext.stream, result);
  assert.equal(sinkContext.logger, logger);
  assert.deepEqual(sourceValues, ["request"]);
  assert.deepEqual(sourceErrors, [failure]);
  assert.deepEqual(sinkResults, [42]);
  assert.deepEqual(sinkErrors, [failure]);
});

await test("structured log fields preserve canonical typed values without formatting", () => {
  const failure = new Error("failed");
  const object = { id: 1 };

  assert.deepEqual(str("name", "order"), {
    key: "name",
    type: "string",
    value: "order"
  });
  assert.deepEqual(int("count", 2), { key: "count", type: "int", value: 2 });
  assert.deepEqual(int64("offset", 9n), { key: "offset", type: "int64", value: 9n });
  assert.deepEqual(float64("rate", 1.5), { key: "rate", type: "float64", value: 1.5 });
  assert.deepEqual(bool("ready", true), { key: "ready", type: "bool", value: true });
  assert.deepEqual(err(failure), { key: "error", type: "error", value: failure });
  assert.deepEqual(any("payload", object), { key: "payload", type: "any", value: object });
  assert.throws(() => int("unsafe", Number.MAX_SAFE_INTEGER + 1), /safe integer/);
});
