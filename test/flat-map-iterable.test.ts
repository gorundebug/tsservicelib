import assert from "node:assert/strict";
import { test } from "node:test";

import {
  makeFlatMapIterableStream,
  type IndexedIterable
} from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  stringSerdeType,
  type FlatMapIterableStreamConfig,
  type StreamConfig,
  type TransformationType,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import {
  makeTestEnvironment,
  makeTestSerde,
  registerTestSerdeType
} from "./support/environment.js";

function config<const T extends TransformationType>(
  id: number,
  name: string,
  type: T
): StreamConfig & { readonly type: T } {
  return {
    id,
    name,
    type,
    properties: {},
    pipeline: "main",
    idService: 1,
    idSource: id - 1,
    idSources: [],
    xPos: id,
    yPos: 0
  };
}

function iterableConfig(valueType: string): FlatMapIterableStreamConfig {
  return {
    ...config(2, "items", "FlatMapIterable"),
    type: "FlatMapIterable",
    valueType
  };
}

class RecordingConsumer<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

await test("FlatMapIterable emits indexed array items in order without copying", async () => {
  const sourceConfig = config(1, "source", "Input");
  const streamConfig = iterableConfig("test.IterableItem");
  const resultConfig = config(3, "result", "Sink");
  const environment = makeTestEnvironment([sourceConfig, streamConfig, resultConfig]);
  registerTestSerdeType(
    environment,
    "test.IterableItem",
    (value): value is { id: number } =>
      typeof value === "object" && value !== null && "id" in value && typeof value.id === "number"
  );
  const source = new ConsumedStream<IndexedIterable<{ id: number }>>(
    sourceConfig,
    environment,
    makeTestSerde()
  );
  const stream = makeFlatMapIterableStream(streamConfig, source);
  const result = new RecordingConsumer<{ id: number }>(resultConfig, environment);
  stream.setConsumer(result);
  const first = { id: 1 };
  const second = { id: 2 };

  await source.emit(new MessageContext(), [first, second]);

  assert.deepEqual(result.values, [first, second]);
  assert.equal(result.values[0], first);
});

await test("FlatMapIterable string int32 mode emits Unicode code points", async () => {
  const sourceConfig = config(1, "source", "Input");
  const streamConfig = { ...iterableConfig("int32"), valueType: "int32" } as const;
  const resultConfig = config(3, "result", "Sink");
  const environment = makeTestEnvironment([sourceConfig, streamConfig, resultConfig]);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  const stream = makeFlatMapIterableStream(streamConfig, source);
  const result = new RecordingConsumer<number>(resultConfig, environment);
  stream.setConsumer(result);

  await source.emit(new MessageContext(), "A😀");

  assert.deepEqual(result.values, [65, 0x1f600]);
});

await test("FlatMapIterable string uint8 mode emits UTF-8 bytes", async () => {
  const sourceConfig = config(1, "source", "Input");
  const streamConfig = { ...iterableConfig("uint8"), valueType: "uint8" } as const;
  const resultConfig = config(3, "result", "Sink");
  const environment = makeTestEnvironment([sourceConfig, streamConfig, resultConfig]);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  const stream = makeFlatMapIterableStream(streamConfig, source);
  const result = new RecordingConsumer<number>(resultConfig, environment);
  stream.setConsumer(result);

  await source.emit(new MessageContext(), "é");

  assert.deepEqual(result.values, [0xc3, 0xa9]);
});
