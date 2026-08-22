import assert from "node:assert/strict";
import { test } from "node:test";

import {
  makeFilterStream,
  makeKeyByStream,
  makeLinkStream,
  makeMapStream,
  makeProcessStream,
  makeSplitStream
} from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  int32ArraySerdeType,
  int32SerdeType,
  MessageContext,
  stringSerdeType,
  type CycleLinkStreamConfig,
  type FilterStreamConfig,
  type KeyByStreamConfig,
  type MapStreamConfig,
  type ProcessStreamConfig,
  type SplitStreamConfig,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

await test("same-type operators retain the exact source StreamSerde instance", () => {
  const sourceConfig = config(1, "source", "Input");
  const filterConfig: FilterStreamConfig = config(2, "filter", "Filter", 1);
  const environment = makeTestEnvironment([sourceConfig, filterConfig]);
  const sourceSerde = environment.serde(int32SerdeType);
  const source = new ConsumedStream(sourceConfig, environment, sourceSerde);
  const filter = makeFilterStream(filterConfig, source, {
    filter(): boolean {
      return true;
    }
  });

  assert.equal(source.serde(), sourceSerde);
  assert.equal(filter.serde(), sourceSerde);
  assert.equal(
    hex(environment.serde(int32ArraySerdeType).serialize([0, -1])),
    "0000000000000002800000007fffffff"
  );
});

await test("type-changing and KeyValue operators resolve the declared output serde", () => {
  const sourceConfig = config(1, "source", "Input");
  const mapConfig: MapStreamConfig = {
    ...config(2, "map", "Map", 1),
    valueType: "string"
  };
  const environment = makeTestEnvironment([sourceConfig, mapConfig]);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  const map = makeMapStream(mapConfig, source, {
    map(context, _stream, value, out) {
      return out.out(context, String(value));
    }
  });
  assert.equal(map.serde(), environment.serde(stringSerdeType));

  const keySourceConfig = config(10, "key-source", "Input");
  const keyByConfig: KeyByStreamConfig = {
    ...config(11, "key-by", "KeyBy", 10),
    keyType: "int32",
    valueType: "string"
  };
  const keyEnvironment = makeTestEnvironment([keySourceConfig, keyByConfig]);
  const keySource = new ConsumedStream(
    keySourceConfig,
    keyEnvironment,
    keyEnvironment.serde(stringSerdeType)
  );
  const keyBy = makeKeyByStream(keyByConfig, keySource, {
    keyBy(context, _stream, value, out) {
      return out.out(context, { key: value.length, value });
    }
  });
  const value = { key: 3, value: "abc" };
  assert.equal(hex(keyBy.serde().serializeKey(value)), "80000003");
  assert.equal(hex(keyBy.serde().serializeValue(value)), "0000000000000003616263");
  assert.deepEqual(
    keyBy
      .serde()
      .deserializeKeyValue(keyBy.serde().serializeKey(value), keyBy.serde().serializeValue(value)),
    value
  );
});

await test("cycle, split, process result and virtual error outputs expose their own serde", async () => {
  const cycleConfig: CycleLinkStreamConfig = {
    ...config(1, "cycle", "CycleLink")
  };
  const environment = makeTestEnvironment([cycleConfig]);
  environment.serdeRegistry().registerStreamValueType(cycleConfig.id, stringSerdeType);
  const cycle = makeLinkStream<string>(cycleConfig, environment);
  assert.equal(cycle.serde(), environment.serde(stringSerdeType));

  const sourceConfig = config(10, "source", "Input");
  const splitConfig: SplitStreamConfig = config(11, "split", "Split", 10);
  const splitEnvironment = makeTestEnvironment([sourceConfig, splitConfig]);
  const source = new ConsumedStream(
    sourceConfig,
    splitEnvironment,
    splitEnvironment.serde(int32SerdeType)
  );
  const split = makeSplitStream(splitConfig, source);
  assert.equal(split.addStream().serde(), source.serde());

  const processConfig: ProcessStreamConfig = {
    ...config(21, "process", "Process", 20)
  };
  const processSourceConfig = config(20, "process-source", "Input");
  const processEnvironment = makeTestEnvironment([processSourceConfig, processConfig]);
  processEnvironment.serdeRegistry().registerStreamValueType(processConfig.id, int32SerdeType);
  processEnvironment.serdeRegistry().registerStreamErrorType(processConfig.id, stringSerdeType);
  const processSource = new ConsumedStream(
    processSourceConfig,
    processEnvironment,
    processEnvironment.serde(stringSerdeType)
  );
  const process = makeProcessStream(processConfig, processSource, {
    process(context, _stream, value, out, errorOut) {
      return value.length > 0 ? out.out(context, value.length) : errorOut.out(context, "empty");
    }
  });
  assert.equal(process.serde(), processEnvironment.serde(int32SerdeType));
  assert.equal(process.errorStream().serde(), processEnvironment.serde(stringSerdeType));
  await processSource.emit(new MessageContext(), "value");
});

function config<T extends StreamConfig["type"]>(
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

function hex(value: Uint8Array | undefined): string {
  if (value === undefined) {
    throw new Error("expected serialized key");
  }
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("hex");
}
