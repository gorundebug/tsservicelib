import assert from "node:assert/strict";
import { setTimeout as wait } from "node:timers/promises";
import { test } from "node:test";

import { makeDelayStream, type DelayFunction } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  DelayPool,
  DurableCallContext,
  MessageContext,
  ServiceStream,
  type DelayStreamConfig,
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

class RecordingStream<T> extends ServiceStream implements TypedStreamConsumer<T> {
  public readonly values: T[] = [];

  public consume(_context: MessageContext, value: T): void {
    this.values.push(value);
  }
}

function setup(function_: DelayFunction<number>, pool = new DelayPool()) {
  const sourceConfig = streamConfig(1, "source");
  const delayConfig: DelayStreamConfig = {
    ...streamConfig(2, "delay"),
    type: "Delay",
    idSource: 1,
    duration: 0
  };
  const outputConfig = streamConfig(3, "output");
  const environment = makeTestEnvironment([sourceConfig, delayConfig, outputConfig], {
    delayPool: pool
  });
  const source = new ConsumedStream<number>(sourceConfig, environment, makeTestSerde());
  const delay = makeDelayStream(delayConfig, source, function_);
  const output = new RecordingStream<number>(outputConfig, environment);
  delay.setConsumer(output);
  return { delay, environment, output, source };
}

await test("positive delay schedules and later emits without blocking consume", async () => {
  const { environment, output, source } = setup({
    duration(): number {
      return 10;
    },
    delayError(): void {
      return undefined;
    }
  });
  const completion = source.emit(new MessageContext(), 1);
  await completion;
  assert.deepEqual(output.values, []);
  await wait(25);
  assert.deepEqual(output.values, [1]);
  await environment.delayPool().stop(Context.background());
});

await test("non-positive delay emits even when the context is already cancelled", async () => {
  const controller = new AbortController();
  controller.abort(new Error("cancelled"));
  const { output, source } = setup({
    duration(): number {
      return 0;
    },
    delayError(): void {
      return undefined;
    }
  });
  await source.emit(new MessageContext(controller.signal), 1);
  assert.deepEqual(output.values, [1]);
});

await test("positive delay skips downstream when cancellation wins", async () => {
  const controller = new AbortController();
  const { environment, output, source } = setup({
    duration(): number {
      return 100;
    },
    delayError(): void {
      return undefined;
    }
  });
  await source.emit(new MessageContext(controller.signal), 1);
  controller.abort(new Error("cancelled"));
  await wait(10);
  assert.deepEqual(output.values, []);
  await environment.delayPool().stop(Context.background());
});

await test("delay scheduling rejection invokes delayError with the normal collector", async () => {
  const pool = new DelayPool();
  await pool.stop(Context.background());
  const errors: unknown[] = [];
  const { output, source } = setup(
    {
      duration(): number {
        return 10;
      },
      delayError(context, _stream, value, error, out) {
        errors.push(error);
        return out.out(context, value + 1);
      }
    },
    pool
  );

  await source.emit(new MessageContext(), 1);
  assert.equal(errors.length, 1);
  assert.deepEqual(output.values, [2]);
});

await test("Workflow delay uses the durable timer and awaits it before emitting", async () => {
  const delays: number[] = [];
  const { output, source } = setup({
    duration(): number {
      return 250;
    },
    delayError(): void {
      return undefined;
    }
  });
  const durable = new DurableCallContext("workflow-message", "Workflow", {
    timer: (delayMs) => {
      delays.push(delayMs);
      return Promise.resolve();
    }
  });
  await source.emit(new MessageContext().withDurableCallContext(durable), 1);
  assert.deepEqual(delays, [250]);
  assert.deepEqual(output.values, [1]);
});
