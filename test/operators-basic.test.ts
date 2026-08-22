import assert from "node:assert/strict";
import { test } from "node:test";

import {
  makeFilterStream,
  makeFlatMapStream,
  makeKeyByStream,
  makeMapStream,
  makeProcessStream,
  type FilterFunction,
  type FlatMapFunction,
  type KeyByFunction,
  type MapFunction,
  type ProcessFunction
} from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  int32SerdeType,
  MessageContext,
  ServiceStream,
  stringSerdeType,
  type Completion,
  type KeyValue,
  type StreamConfig,
  type TransformationType,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

function streamConfig<const T extends TransformationType>(
  id: number,
  name: string,
  type: T,
  idSource = 0
): StreamConfig & { readonly type: T } {
  return {
    id,
    name,
    properties: {},
    type,
    pipeline: "main",
    idService: 1,
    idSource,
    idSources: [],
    xPos: id,
    yPos: 0
  };
}

class RecordingConsumer<T> extends ServiceStream implements TypedStreamConsumer<T> {
  readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

await test("map filter and flat-map preserve collector and stream function contracts", async () => {
  const sourceConfig = streamConfig(1, "input", "Input");
  const mapConfig = { ...streamConfig(2, "double", "Map", 1), valueType: "int32" } as const;
  const filterConfig = streamConfig(3, "positive", "Filter", 2);
  const flatMapConfig = {
    ...streamConfig(4, "expand", "FlatMap", 3),
    valueType: "string"
  } as const;
  const resultConfig = streamConfig(5, "result", "Sink", 4);
  const environment = makeTestEnvironment([
    sourceConfig,
    mapConfig,
    filterConfig,
    flatMapConfig,
    resultConfig
  ]);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  let mapStreamSeen: string | undefined;
  const mapFunction: MapFunction<number, number> = {
    map(context, stream, value, out): Completion {
      mapStreamSeen = stream.name;
      return out.out(context, value * 2);
    }
  };
  const map = makeMapStream(mapConfig, source, mapFunction);
  const filterFunction: FilterFunction<number> = {
    filter(_context, stream, value): boolean {
      assert.equal(stream.name, "positive");
      return value > 2;
    }
  };
  const filter = makeFilterStream(filterConfig, map, filterFunction);
  const flatMapFunction: FlatMapFunction<number, string> = {
    async flatMap(context, stream, value, out): Promise<void> {
      assert.equal(stream.name, "expand");
      await out.out(context, String(value));
      await out.out(context, String(value + 1));
    }
  };
  const flatMap = makeFlatMapStream(flatMapConfig, filter, flatMapFunction);
  const result = new RecordingConsumer<string>(resultConfig, environment);
  flatMap.setConsumer(result);

  const context = new MessageContext();
  await source.emit(context, 1);
  await source.emit(context, 2);

  assert.equal(mapStreamSeen, "double");
  assert.deepEqual(result.values, ["4", "5"]);
});

await test("key-by emits the canonical key/value shape", async () => {
  const sourceConfig = streamConfig(10, "input", "Input");
  const keyByConfig = {
    ...streamConfig(11, "keyByLength", "KeyBy", 10),
    keyType: "int32",
    valueType: "string"
  } as const;
  const resultConfig = streamConfig(12, "result", "Sink", 11);
  const environment = makeTestEnvironment([sourceConfig, keyByConfig, resultConfig]);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(stringSerdeType));
  const function_: KeyByFunction<string, number, string> = {
    keyBy(context, stream, value, out): Completion {
      assert.equal(stream.name, "keyByLength");
      return out.out(context, { key: value.length, value });
    }
  };
  const keyBy = makeKeyByStream(keyByConfig, source, function_);
  const result = new RecordingConsumer<KeyValue<number, string>>(resultConfig, environment);
  keyBy.setConsumer(result);

  await source.emit(new MessageContext(), "abc");
  assert.deepEqual(result.values, [{ key: 3, value: "abc" }]);
});

await test("process has independent result and virtual negative-id error outputs", async () => {
  const sourceConfig = streamConfig(20, "input", "Input");
  const processConfig = {
    ...streamConfig(21, "process", "Process", 20)
  } as const;
  const resultConfig = streamConfig(22, "result", "Sink", 21);
  const errorConfig = streamConfig(23, "errors", "Sink", 21);
  const environment = makeTestEnvironment([sourceConfig, processConfig, resultConfig, errorConfig]);
  environment.serdeRegistry().registerStreamValueType(processConfig.id, int32SerdeType);
  environment.serdeRegistry().registerStreamErrorType(processConfig.id, stringSerdeType);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  const function_: ProcessFunction<number, number, string> = {
    process(context, stream, value, out, errorOut): Completion {
      assert.equal(stream.name, "process");
      return value >= 0
        ? out.out(context, value * 10)
        : errorOut.out(context, `negative:${String(value)}`);
    }
  };
  const process = makeProcessStream(processConfig, source, function_);
  const results = new RecordingConsumer<number>(resultConfig, environment);
  const errors = new RecordingConsumer<string>(errorConfig, environment);
  process.setConsumer(results);
  process.errorStream().setConsumer(errors);

  const context = new MessageContext();
  await source.emit(context, 2);
  await source.emit(context, -1);

  assert.deepEqual(results.values, [20]);
  assert.deepEqual(errors.values, ["negative:-1"]);
  assert.equal(process.errorStream().id, -process.id);
  assert.equal(process.errorStream().name, process.name);
  assert.equal(process.errorStream().transformationName, process.transformationName);
});
