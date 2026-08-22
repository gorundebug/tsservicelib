import assert from "node:assert/strict";
import { test } from "node:test";

import { makeSplitStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  FunctionCaller,
  MessageContext,
  ServiceStream,
  type Caller,
  type CallerFactory,
  type SplitStreamConfig,
  type Stream,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, makeTestSerde } from "./support/environment.js";

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

class ClassifiedCallerFactory implements CallerFactory {
  public create<T>(_source: Stream, consumer: TypedStreamConsumer<T>): Caller<T> {
    return new FunctionCaller(consumer, consumer.name.startsWith("async"));
  }
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  readonly #events: string[];
  public readonly records: { readonly context: MessageContext; readonly value: T }[] = [];

  public constructor(
    config: StreamConfig,
    environment: ReturnType<typeof makeTestEnvironment>,
    events: string[]
  ) {
    super(config, environment);
    this.#events = events;
  }

  public consume(context: MessageContext, value: T): void {
    this.#events.push(this.name);
    this.records.push({ context, value });
  }
}

await test("split validates branches once and dispatches async branches first stably", () => {
  const sourceConfig = streamConfig(1, "source");
  const splitConfig: SplitStreamConfig = {
    ...streamConfig(2, "fanOut"),
    type: "Split",
    idSource: 1
  };
  const directConfig = streamConfig(3, "direct");
  const asyncFirstConfig = streamConfig(4, "async-first");
  const asyncSecondConfig = streamConfig(5, "async-second");
  const environment = makeTestEnvironment(
    [sourceConfig, splitConfig, directConfig, asyncFirstConfig, asyncSecondConfig],
    { callerFactory: new ClassifiedCallerFactory() }
  );
  const source = new ConsumedStream<{ id: number }>(sourceConfig, environment, makeTestSerde());
  const split = makeSplitStream(splitConfig, source);
  const directLink = split.addStream();
  const asyncFirstLink = split.addStream();
  const asyncSecondLink = split.addStream();
  assert.equal(directLink.name, "fanOutSplitLink0");
  assert.equal(asyncFirstLink.name, "fanOutSplitLink1");
  const events: string[] = [];
  const direct = new RecordingStream(directConfig, environment, events);
  const asyncFirst = new RecordingStream(asyncFirstConfig, environment, events);
  const asyncSecond = new RecordingStream(asyncSecondConfig, environment, events);
  directLink.setConsumer(direct);
  asyncFirstLink.setConsumer(asyncFirst);
  asyncSecondLink.setConsumer(asyncSecond);
  split.build();

  const context = new MessageContext();
  const value = { id: 1 };
  const completion = source.emit(context, value);
  assert.equal(completion, undefined);

  assert.deepEqual(events, ["async-first", "async-second", "direct"]);
  for (const branch of [asyncFirst, asyncSecond, direct]) {
    const record = branch.records[0];
    assert.ok(record !== undefined);
    assert.equal(record.context, context);
    assert.equal(record.value, value);
  }
  assert.deepEqual(split.consumers(), [direct, asyncFirst, asyncSecond]);
  assert.deepEqual(environment.buildables(), [split]);
});

await test("split build reports the exact unbound branch", () => {
  const sourceConfig = streamConfig(1, "source");
  const splitConfig: SplitStreamConfig = {
    ...streamConfig(2, "fanOut"),
    type: "Split",
    idSource: 1
  };
  const environment = makeTestEnvironment([sourceConfig, splitConfig]);
  const source = new ConsumedStream<number>(sourceConfig, environment, makeTestSerde());
  const split = makeSplitStream(splitConfig, source);
  split.addStream();

  assert.throws(() => {
    split.build();
  }, /link with index 0.*does not have consumer/);
});
