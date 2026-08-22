import assert from "node:assert/strict";
import { test } from "node:test";

import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  type Completion,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

class NumberConsumer extends ServiceStream implements TypedStreamConsumer<number> {
  readonly values: number[] = [];

  public consume(_context: MessageContext, value: number): Completion {
    this.values.push(value);
  }
}

const sourceConfig: StreamConfig = {
  id: 1,
  name: "source",
  properties: {},
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0
};

await test("consumed stream binds exactly one consumer and forwards identity", async () => {
  const consumerConfig: StreamConfig = {
    id: 2,
    name: "consumer",
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: 1,
    idSources: [],
    xPos: 1,
    yPos: 0
  };
  const environment = makeTestEnvironment([sourceConfig, consumerConfig]);
  const stream = new ConsumedStream<number>(sourceConfig, environment, makeTestSerde());
  const consumer = new NumberConsumer(consumerConfig, environment);
  stream.setConsumer(consumer);

  await stream.emit(new MessageContext(), 42);
  assert.deepEqual(consumer.values, [42]);
  assert.deepEqual(stream.consumers(), [consumer]);
  assert.equal(stream.consumer(), consumer);
  assert.throws(() => {
    stream.setConsumer(consumer);
  }, /consumer already assigned/);
});

await test("unbound collector remains a no-op like the canonical collector", async () => {
  const stream = new ConsumedStream<number>(
    sourceConfig,
    makeTestEnvironment([sourceConfig]),
    makeTestSerde()
  );
  await stream.collector().out(new MessageContext(), 1);
  assert.deepEqual(stream.consumers(), []);
});
