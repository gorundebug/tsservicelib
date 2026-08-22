import assert from "node:assert/strict";
import { test } from "node:test";

import { makeLinkStream, makeMapStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  int32SerdeType,
  MessageContext,
  type CycleLinkStreamConfig,
  type MapStreamConfig,
  type RuntimeBuildable,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

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

await test("environment builds registered graph helpers in declaration order", async () => {
  const environment = makeTestEnvironment([]);
  const events: number[] = [];
  for (const value of [1, 2, 3]) {
    const buildable: RuntimeBuildable = {
      build(): void {
        events.push(value);
      }
    };
    environment.registerRuntimeBuildable(buildable);
  }

  await environment.buildRuntimeStreams();
  assert.deepEqual(events, [1, 2, 3]);
});

await test("runtime topology reflects late cycle binding and validates config edges", async () => {
  const sourceConfig = streamConfig(1, "source");
  const cycleConfig: CycleLinkStreamConfig = {
    ...streamConfig(2, "cycle"),
    type: "CycleLink",
    idSource: sourceConfig.id
  };
  const mapConfig: MapStreamConfig = {
    ...streamConfig(3, "mapped"),
    type: "Map",
    idSource: cycleConfig.id,
    valueType: "int32"
  };
  const environment = makeTestEnvironment([sourceConfig, cycleConfig, mapConfig]);
  environment.serdeRegistry().registerStreamValueType(cycleConfig.id, int32SerdeType);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  environment.registerStream(source);
  const cycle = makeLinkStream<number>(cycleConfig, environment);
  const mapped = makeMapStream<number, number>(mapConfig, cycle, {
    map(context, _stream, value, out) {
      return out.out(context, value + 1);
    }
  });
  cycle.setSource(source);

  await environment.buildRuntimeStreams();
  environment.validateRuntimeTopology();

  assert.deepEqual(
    [...environment.runtimeStreamIds()].sort((a, b) => a - b),
    [1, 2, 3]
  );
  assert.deepEqual(environment.graphLinks(), [
    { from: 1, to: 2 },
    { from: 2, to: 3 }
  ]);
  await source.emit(new MessageContext(), 1);
  assert.equal(mapped.id, 3);
});

await test("runtime topology rejects an unbound configured cycle edge", () => {
  const sourceConfig = streamConfig(10, "source");
  const cycleConfig: CycleLinkStreamConfig = {
    ...streamConfig(11, "cycle"),
    type: "CycleLink",
    idSource: sourceConfig.id
  };
  const environment = makeTestEnvironment([sourceConfig, cycleConfig]);
  environment.serdeRegistry().registerStreamValueType(cycleConfig.id, int32SerdeType);
  environment.registerStream(
    new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType))
  );
  makeLinkStream<number>(cycleConfig, environment);

  assert.throws(() => {
    environment.validateRuntimeTopology();
  }, /runtime graph link from=10 to=11 is missing/);
});
