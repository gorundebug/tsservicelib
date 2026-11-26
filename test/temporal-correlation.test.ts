import assert from "node:assert/strict";
import { test } from "node:test";

import { makeTemporalEndpointConsumerWithHandler } from "@gorundebug/tsservicelib/datasource/temporal";
import {
  MessageContext,
  noopLogger,
  noopMetrics,
  type Consumer,
  type RuntimeEnvironment,
  type TypedInputStream
} from "@gorundebug/tsservicelib/runtime";
import type { EndpointEnvelope, EndpointResult } from "../src/datasource/temporal/contracts.js";

function fixture() {
  const calls: { context: MessageContext; value: string }[] = [];
  let resultConsumer: Consumer<string> | undefined;
  const environment = {
    runtimeConfig: () => ({
      dataConnectorById: () => ({ id: 1, name: "temporal", type: 6 }),
      endpointById: () => ({
        id: 1,
        name: "concurrent",
        idDataConnector: 1,
        enabled: true,
        schedule: "",
        timezone: "UTC",
        overlapPolicy: "Allow",
        missedRunPolicy: "Skip",
        taskQueue: "test",
        temporalExecutionType: "Activity",
        maxConcurrentActivities: 2,
        maxConcurrentWorkflowTasks: 0,
        scheduleId: "",
        workflowExecutionTimeout: 0,
        activityStartToCloseTimeout: 1000,
        activityHeartbeatTimeout: 0,
        maximumAttempts: 1
      }),
      streamById: () => undefined
    }),
    managedDataConnectorById: () => undefined,
    addManagedDataConnector: () => undefined,
    dataSourceById: () => undefined,
    addDataSource: () => undefined,
    tracing: () => undefined,
    metrics: () => noopMetrics,
    log: () => noopLogger
  } as unknown as RuntimeEnvironment;
  const stream = {
    id: 1,
    name: "input",
    runtimeEnvironment: () => environment,
    endpointId: () => 1,
    serde: () => ({ deserialize: (bytes: Uint8Array) => new TextDecoder().decode(bytes) }),
    resultStream: () => ({
      serde: () => ({ serialize: (value: string) => new TextEncoder().encode(value) })
    }),
    setResultConsumer: (consumer: Consumer<string>) => {
      resultConsumer = consumer;
    }
  } as unknown as TypedInputStream<string, string, string>;
  const consumer = makeTemporalEndpointConsumerWithHandler(stream, {
    beginRequest: (context) => ({ context, state: undefined }),
    consumeMessage: (context, _stream, _state, value) => {
      assert.equal(context.streamId(), "shared-parent");
      calls.push({ context, value });
      if (value === "fail") throw new Error("handler failed");
    },
    endRequest: () => undefined
  }) as unknown as {
    activate(envelope: EndpointEnvelope, context: MessageContext): Promise<EndpointResult>;
  };
  return {
    consumer,
    calls,
    reply: (context: MessageContext, value: string) => resultConsumer?.consume(context, value)
  };
}

function envelope(value: string): EndpointEnvelope {
  return {
    version: 1,
    endpointId: 1,
    streamId: "shared-parent",
    messageId: "shared-parent",
    priority: 0,
    deadlineUnixMillis: 0,
    scheduled: false,
    scheduleId: "",
    scheduledAtUnixMillis: 0,
    firedAtUnixMillis: 0,
    payload: new TextEncoder().encode(value)
  };
}

await test("concurrent Temporal invocations preserve parent and isolate replies", async () => {
  const { consumer, calls, reply } = fixture();
  const first = consumer.activate(envelope("first"), new MessageContext());
  const second = consumer.activate(envelope("second"), new MessageContext());
  // Both beginRequest continuations run before observing their pending replies.
  await Promise.resolve();
  assert.equal(calls.length, 2);
  const [one, two] = calls;
  assert.ok(one);
  assert.ok(two);
  await reply(two.context, "second-result");
  await reply(two.context, "duplicate");
  await reply(one.context, "first-result");
  const [a, b] = await Promise.all([first, second]);
  assert.equal(new TextDecoder().decode(a.payload), "first-result");
  assert.equal(new TextDecoder().decode(b.payload), "second-result");
});

await test("late Temporal reply after failed invocation cannot complete its replacement", async () => {
  const { consumer, calls, reply } = fixture();
  await assert.rejects(
    consumer.activate(envelope("fail"), new MessageContext()),
    /handler failed/u
  );
  const old = calls[0];
  assert.ok(old);
  const next = consumer.activate(envelope("replacement"), new MessageContext());
  await Promise.resolve();
  const current = calls[1];
  assert.ok(current);
  await reply(old.context, "stale");
  await reply(current.context, "current");
  const result = await next;
  assert.equal(new TextDecoder().decode(result.payload), "current");
});
