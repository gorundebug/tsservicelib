import assert from "node:assert/strict";
import { test } from "node:test";

import {
  DurableCallContext,
  DurableCallHeartbeatAfterCompletionError,
  MessageContext,
  SpanStatusCode,
  bindDurableCallSpan,
  durableCallHeartbeat,
  runDurableCallActivity
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

await test("heartbeat outside Temporal is a silent no-op", () => {
  durableCallHeartbeat(new MessageContext(), "ignored");
});

await test("Activity returns the handler result and closes successfully", async () => {
  const events: string[] = [];
  const durable = new DurableCallContext("message-1", undefined, (event) => {
    events.push(event);
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
  const durable = new DurableCallContext(
    "message-1",
    (message) => heartbeats.push(message),
    (event) => events.push(event)
  );
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
  const durable = new DurableCallContext("message-1");
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
