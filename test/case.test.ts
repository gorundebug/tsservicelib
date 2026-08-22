import assert from "node:assert/strict";
import { test } from "node:test";

import {
  makeCaseStream,
  makeWhenStream,
  defaultBuildSwitch,
  type BuildSwitchFunction,
  type When
} from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  type CaseStreamConfig,
  type Stream,
  type StreamConfig,
  type TypedStreamConsumer,
  type WhenStreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

type Event =
  | { readonly kind: "created"; readonly id: number }
  | { readonly kind: "failed"; readonly reason: string };

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

function whenConfig(id: number, name: string, valueType: string): WhenStreamConfig {
  return {
    ...streamConfig(id, name),
    type: "When",
    idSource: 2,
    valueType
  };
}

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

class EventSwitch implements BuildSwitchFunction<Event> {
  public seenWhen: readonly When[] = [];

  public buildSwitch(stream: Stream, whenItems: readonly When[]): (value: Event) => number {
    assert.equal(stream.name, "eventCase");
    this.seenWhen = whenItems;
    const byType = new Map(whenItems.map((item, index) => [item.valueType(), index]));
    return (value) => {
      const index = byType.get(value.kind);
      if (index === undefined) {
        throw new Error(`unknown event kind ${value.kind}`);
      }
      return index;
    };
  }
}

await test("case builds one selector and routes the exact value to its typed when branch", async () => {
  const sourceConfig = streamConfig(1, "source");
  const caseConfig: CaseStreamConfig = {
    ...streamConfig(2, "eventCase"),
    type: "Case",
    idSource: 1
  };
  const createdConfig = whenConfig(3, "created", "created");
  const failedConfig = whenConfig(4, "", "failed");
  const createdSinkConfig = streamConfig(5, "createdSink");
  const failedSinkConfig = streamConfig(6, "failedSink");
  const environment = makeTestEnvironment([
    sourceConfig,
    caseConfig,
    createdConfig,
    failedConfig,
    createdSinkConfig,
    failedSinkConfig
  ]);
  const eventType = registerTestSerdeType(environment, "test.Event", isEvent);
  registerTestSerdeType(
    environment,
    "created",
    (value): value is Extract<Event, { kind: "created" }> =>
      isEvent(value) && value.kind === "created"
  );
  registerTestSerdeType(
    environment,
    "failed",
    (value): value is Extract<Event, { kind: "failed" }> =>
      isEvent(value) && value.kind === "failed"
  );
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(eventType));
  const buildSwitch = new EventSwitch();
  const caseStream = makeCaseStream(caseConfig, source, buildSwitch);
  const created = makeWhenStream<Event, Extract<Event, { kind: "created" }>>(
    createdConfig,
    caseStream
  );
  const failed = makeWhenStream<Event, Extract<Event, { kind: "failed" }>>(
    failedConfig,
    caseStream
  );
  const createdSink = new RecordingStream<Extract<Event, { kind: "created" }>>(
    createdSinkConfig,
    environment
  );
  const failedSink = new RecordingStream<Extract<Event, { kind: "failed" }>>(
    failedSinkConfig,
    environment
  );
  created.setConsumer(createdSink);
  failed.setConsumer(failedSink);
  caseStream.build();
  const createdValue = { kind: "created", id: 1 } as const;
  const failedValue = { kind: "failed", reason: "invalid" } as const;

  await source.emit(new MessageContext(), createdValue);
  await source.emit(new MessageContext(), failedValue);

  assert.equal(createdSink.values[0], createdValue);
  assert.equal(failedSink.values[0], failedValue);
  assert.equal(failed.name, "eventCaseCaseLink1");
  assert.deepEqual(caseStream.consumers(), [created, failed]);
  assert.deepEqual(
    buildSwitch.seenWhen.map((item) => item.whenConsumer()),
    [createdSink, failedSink]
  );
  assert.deepEqual(environment.buildables(), [caseStream]);
});

await test("case rejects consumption before build and selector indices outside branches", () => {
  const sourceConfig = streamConfig(1, "source");
  const caseConfig: CaseStreamConfig = {
    ...streamConfig(2, "eventCase"),
    type: "Case",
    idSource: 1
  };
  const when = whenConfig(3, "created", "created");
  const environment = makeTestEnvironment([sourceConfig, caseConfig, when]);
  const eventType = registerTestSerdeType(environment, "created", isEvent);
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(eventType));
  const caseStream = makeCaseStream(caseConfig, source, {
    buildSwitch(): (value: Event) => number {
      return () => 5;
    }
  });
  makeWhenStream(when, caseStream);

  assert.throws(() => {
    void source.emit(new MessageContext(), { kind: "created", id: 1 });
  }, /not built/);
  caseStream.build();
  assert.throws(() => {
    void source.emit(new MessageContext(), { kind: "created", id: 1 });
  }, /only 1 branches exist/);
});

await test("default case switch uses registered runtime types with Go-compatible last match", () => {
  const sourceConfig = streamConfig(1, "source");
  const environment = makeTestEnvironment([sourceConfig]);
  registerTestSerdeType(environment, "event", (value): value is Event => isEvent(value));
  registerTestSerdeType(
    environment,
    "created",
    (value): value is Extract<Event, { kind: "created" }> =>
      isEvent(value) && value.kind === "created"
  );
  const source = new ConsumedStream(
    sourceConfig,
    environment,
    environment.serdeByName<Event>("event")
  );
  const branches: readonly When[] = [
    { valueType: () => "event", whenConsumer: () => source },
    { valueType: () => "created", whenConsumer: () => source }
  ];
  const select = defaultBuildSwitch(source, branches);

  assert.equal(select({ kind: "created", id: 1 }), 1);
  assert.equal(select({ kind: "failed", reason: "invalid" }), 0);
  assert.throws(() => select({ kind: "unknown" }), /unknown value type in case switch/);
});

function isEvent(value: unknown): value is Event {
  if (typeof value !== "object" || value === null || !("kind" in value)) {
    return false;
  }
  return (
    (value.kind === "created" && "id" in value && typeof value.id === "number") ||
    (value.kind === "failed" && "reason" in value && typeof value.reason === "string")
  );
}
