import assert from "node:assert/strict";
import { test } from "node:test";

import { makeLinkStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  int32SerdeType,
  MessageContext,
  ServiceStream,
  type Completion,
  type CycleLinkStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import {
  makeTestEnvironment,
  makeTestSerde,
  registerTestSerdeType
} from "./support/environment.js";

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

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly records: { readonly context: MessageContext; readonly value: T }[] = [];

  public consume(context: MessageContext, value: T): Completion {
    this.records.push({ context, value });
  }
}

await test("cycle link binds its source late and forwards the exact message", async () => {
  const sourceConfig = streamConfig(1, "cycle-source");
  const linkConfig: CycleLinkStreamConfig = {
    ...streamConfig(2, "cycle-root"),
    type: "CycleLink",
    idSource: sourceConfig.id
  };
  const outputConfig = streamConfig(3, "output");
  const environment = makeTestEnvironment([sourceConfig, linkConfig, outputConfig]);
  const valueType = registerTestSerdeType(
    environment,
    "test.CycleValue",
    (value): value is { readonly id: number } =>
      typeof value === "object" && value !== null && "id" in value && typeof value.id === "number"
  );
  environment.serdeRegistry().registerStreamValueType(linkConfig.id, valueType);
  const link = makeLinkStream<{ readonly id: number }>(linkConfig, environment);
  const output = new RecordingStream<{ readonly id: number }>(outputConfig, environment);
  link.setConsumer(output);

  assert.equal(link.source(), undefined);
  assert.equal(environment.streamById(link.id), link);

  const source = new ConsumedStream(sourceConfig, environment, environment.serde(valueType));
  link.setSource(source);
  const context = new MessageContext().withStreamId("cycle-message");
  const value = { id: 42 };
  await source.emit(context, value);

  assert.equal(link.source(), source);
  assert.deepEqual(link.consumers(), [output]);
  assert.deepEqual(output.records, [{ context, value }]);
  const [record] = output.records;
  assert.ok(record !== undefined);
  assert.equal(record.context, context);
  assert.equal(record.value, value);
  assert.throws(() => {
    link.setSource(source);
  }, /source is already set/);
});

await test("failed late binding does not retain an unconnected source", () => {
  const sourceConfig = streamConfig(10, "occupied-source");
  const linkConfig: CycleLinkStreamConfig = {
    ...streamConfig(11, "cycle-root"),
    type: "CycleLink",
    idSource: sourceConfig.id
  };
  const blockerConfig = streamConfig(12, "blocker");
  const environment = makeTestEnvironment([sourceConfig, linkConfig, blockerConfig]);
  environment.serdeRegistry().registerStreamValueType(linkConfig.id, int32SerdeType);
  const source = new ConsumedStream<number>(sourceConfig, environment, makeTestSerde());
  source.setConsumer(new RecordingStream(blockerConfig, environment));
  const link = makeLinkStream<number>(linkConfig, environment);

  assert.throws(() => {
    link.setSource(source);
  }, /consumer already assigned/);
  assert.equal(link.source(), undefined);
});
