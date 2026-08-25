import assert from "node:assert/strict";
import { test } from "node:test";

import {
  DurableCallAlreadyCompletedError,
  DurableCallContext,
  DurableCallHeartbeatAfterCompletionError,
  DurableCallOutcomeMissingError,
  MessageContext,
  NoDurableCallContextError,
  SpanStatusCode,
  bindDurableCallSpan,
  durableCallError,
  durableCallHeartbeat,
  durableCallSuccess,
  runDurableCallActivity
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

await test("Activity without a deadline waits for an explicit outcome", async () => {
  const durable = new DurableCallContext("parent");
  const entered = Promise.withResolvers<MessageContext>();
  let completed = false;
  const running = runDurableCallActivity(new AbortController().signal, durable, () => {
    entered.resolve(new MessageContext().withDurableCallContext(durable));
    return Promise.resolve();
  }).finally(() => {
    completed = true;
  });

  const context = await entered.promise;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(completed, false);
  durableCallSuccess(context);
  await running;
});

await test("first terminal result wins and heartbeat is accepted only while open", async () => {
  const recorded: unknown[] = [];
  const durable = new DurableCallContext("parent", (message) => {
    recorded.push(message);
  });
  await runDurableCallActivity(new AbortController().signal, durable, () => {
    const context = new MessageContext().withDurableCallContext(durable);
    durableCallHeartbeat(context, "half-way");
    durableCallSuccess(context);
    assert.throws(() => {
      durableCallError(context, new Error("too late"));
    }, DurableCallAlreadyCompletedError);
    assert.throws(() => {
      durableCallHeartbeat(context, "too late");
    }, DurableCallHeartbeatAfterCompletionError);
    return Promise.resolve();
  });
  assert.deepEqual(recorded, ["half-way"]);
});

await test("Activity cancellation supplies the missing-outcome failure", async () => {
  const controller = new AbortController();
  const durable = new DurableCallContext("parent");
  const entered = Promise.withResolvers<undefined>();
  const running = runDurableCallActivity(controller.signal, durable, () => {
    entered.resolve(undefined);
    return Promise.resolve();
  });
  await entered.promise;
  controller.abort(new Error("request deadline"));
  await assert.rejects(running, DurableCallOutcomeMissingError);
});

await test("DurableCall operation outside Activity is observable", () => {
  assert.throws(() => {
    durableCallSuccess(new MessageContext());
  }, NoDurableCallContextError);
});

await test("lifecycle events are attached to the Activity span", async () => {
  const tracing = new TestTracing();
  const durable = new DurableCallContext("parent");
  await runDurableCallActivity(new AbortController().signal, durable, () => {
    const context = new MessageContext().withSampling(true).withDurableCallContext(durable);
    const started = tracing.tracer("service").start(context, "temporal.activity");
    assert.equal(bindDurableCallSpan(started.context, started.span), true);
    durableCallHeartbeat(started.context, "half-way");
    durableCallSuccess(started.context);
    return Promise.resolve();
  });

  const spans = tracing.spans();
  assert.equal(spans.length, 1);
  const [span] = spans;
  assert.ok(span);
  assert.equal(span.statusCode, SpanStatusCode.Ok);
  assert.deepEqual(
    span.events.map(({ name }) => name),
    ["durable_call.heartbeat", "durable_call.success"]
  );
});
