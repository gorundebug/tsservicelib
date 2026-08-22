import assert from "node:assert/strict";
import { test } from "node:test";

import { makeMergeStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  type MergeStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

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
    xPos: id,
    yPos: 0
  };
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly records: { readonly context: MessageContext; readonly value: T }[] = [];

  public consume(context: MessageContext, value: T): void {
    this.records.push({ context, value });
  }
}

await test("merge forwards every source through one identity without copying", async () => {
  const firstConfig = sourceConfig(1);
  const secondConfig = sourceConfig(2);
  const mergeConfig: MergeStreamConfig = {
    ...sourceConfig(3),
    name: "merged",
    type: "Merge",
    idSource: 1,
    idSources: [2]
  };
  const outputConfig = sourceConfig(4);
  const environment = makeTestEnvironment([firstConfig, secondConfig, mergeConfig, outputConfig]);
  const serde = makeTestSerde<{ id: number }>();
  const first = new ConsumedStream(firstConfig, environment, serde);
  const second = new ConsumedStream(secondConfig, environment, serde);
  const merge = makeMergeStream(mergeConfig, first, second);
  const output = new RecordingStream<{ id: number }>(outputConfig, environment);
  merge.setConsumer(output);
  const context = new MessageContext().withStreamId("request");
  const firstValue = { id: 1 };
  const secondValue = { id: 2 };

  await first.emit(context, firstValue);
  await second.emit(context, secondValue);

  assert.deepEqual(
    output.records.map(({ value }) => value),
    [firstValue, secondValue]
  );
  const [firstRecord, secondRecord] = output.records;
  assert.ok(firstRecord !== undefined && secondRecord !== undefined);
  assert.equal(firstRecord.context, context);
  assert.equal(firstRecord.value, firstValue);
  assert.equal(secondRecord.value, secondValue);
  assert.equal(environment.streamById(merge.id), merge);
});
