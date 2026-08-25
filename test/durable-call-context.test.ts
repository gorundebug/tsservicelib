import assert from "node:assert/strict";
import { test } from "node:test";

import {
  DurableCallContext,
  DurableCallContextError,
  DurableCallHeartbeatAfterCompletionError,
  MessageContext,
  SpanStatusCode,
  bindDurableCallSpan,
  durableCallDelay,
  durableCallHeartbeat,
  runDurableCallActivity,
  runDurableCallWorkflow,
  TemporalContinueAsNewRequest,
  temporalContinueAsNew
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

await test("heartbeat outside Temporal is a silent no-op", () => {
  durableCallHeartbeat(new MessageContext(), "ignored");
});

await test("Continue-As-New outside a Workflow is rejected", () => {
  assert.throws(() => temporalContinueAsNew(new MessageContext(), "next"), DurableCallContextError);
});

await test("Continue-As-New is a successful terminal Workflow outcome", async () => {
  const events: string[] = [];
  const durable = new DurableCallContext("message-2", "Workflow", {
    diagnostics: (event) => events.push(event)
  });
  const context = new MessageContext().withDurableCallContext(durable);
  await assert.rejects(
    runDurableCallWorkflow(durable, () => {
      temporalContinueAsNew(context, "next-run");
    }),
    (error: unknown) =>
      error instanceof TemporalContinueAsNewRequest && error.nextInput === "next-run"
  );
  assert.deepEqual(events, ["success"]);
});

await test("Activity returns the handler result and closes successfully", async () => {
  const events: string[] = [];
  const durable = new DurableCallContext("message-1", "Activity", {
    diagnostics: (event) => {
      events.push(event);
    }
  });
  const context = new MessageContext().withDurableCallContext(durable);
  const result = await runDurableCallActivity(durable, () => {
    assert.equal(context.durableCallContext(), durable);
    return Promise.resolve("done");
  });
  assert.equal(result, "done");
  assert.deepEqual(events, ["success"]);
});

await test("Activity records heartbeat and automatic error", async () => {
  const heartbeats: unknown[] = [];
  const events: string[] = [];
  const durable = new DurableCallContext("message-1", "Activity", {
    heartbeat: (message) => heartbeats.push(message),
    diagnostics: (event) => events.push(event)
  });
  const context = new MessageContext().withDurableCallContext(durable);
  await assert.rejects(
    runDurableCallActivity(durable, () => {
      durableCallHeartbeat(context, "half-way");
      return Promise.reject(new Error("business failure"));
    }),
    /business failure/
  );
  assert.deepEqual(heartbeats, ["half-way"]);
  assert.deepEqual(events, ["heartbeat", "error"]);
  assert.throws(() => {
    durable.heartbeat("too late");
  }, DurableCallHeartbeatAfterCompletionError);
});

await test("lifecycle events are attached to the Activity span", async () => {
  const tracing = new TestTracing();
  const durable = new DurableCallContext("message-1", "Activity");
  await runDurableCallActivity(durable, () => {
    const context = new MessageContext().withSampling(true).withDurableCallContext(durable);
    const started = tracing.tracer("service").start(context, "temporal.activity");
    assert.equal(bindDurableCallSpan(started.context, started.span), true);
    durableCallHeartbeat(started.context, "half-way");
    return Promise.resolve();
  });

  const [span] = tracing.spans();
  assert.ok(span);
  assert.equal(span.statusCode, SpanStatusCode.Ok);
  assert.deepEqual(
    span.events.map(({ name }) => name),
    ["temporal.activity.heartbeat", "temporal.activity.success"]
  );
});

await test("Workflow uses its durable timer and heartbeat is a silent no-op", async () => {
  const delays: number[] = [];
  const heartbeats: unknown[] = [];
  const durable = new DurableCallContext("message-2", "Workflow", {
    timer: (delayMs) => {
      delays.push(delayMs);
      return Promise.resolve();
    },
    diagnostics: (event) => heartbeats.push(event)
  });
  const context = new MessageContext().withDurableCallContext(durable);
  const result = await runDurableCallWorkflow(durable, async () => {
    durableCallHeartbeat(context, "ignored");
    assert.equal(await durableCallDelay(context, 1_250), true);
    return "done";
  });
  assert.equal(result, "done");
  assert.deepEqual(delays, [1_250]);
  assert.deepEqual(heartbeats, ["success"]);
});

await test("delay outside a Temporal Workflow is not claimed", async () => {
  assert.equal(await durableCallDelay(new MessageContext(), 10), false);
});
