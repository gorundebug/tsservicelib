import assert from "node:assert/strict";
import { resolve } from "node:path";
import { test } from "node:test";
import { pathToFileURL } from "node:url";

import type { Context as TemporalActivityContext } from "@temporalio/activity";
import type { WorkflowStartInput } from "@temporalio/client";
import { defaultPayloadConverter, type Headers } from "@temporalio/common";

import { MessageContext } from "@gorundebug/tsservicelib/runtime";
import type * as ContextPropagationModule from "../src/datasource/temporal/context-propagation.js";
import type * as WorkflowContextInterceptorModule from "../src/datasource/temporal/workflow-context-interceptor.js";

const propagationUrl = pathToFileURL(
  resolve("dist/datasource/temporal/context-propagation.js")
).href;
const propagation = (await import(propagationUrl)) as typeof ContextPropagationModule;
const workflowInterceptorUrl = pathToFileURL(
  resolve("dist/datasource/temporal/workflow-context-interceptor.js")
).href;
const { interceptors: workflowInterceptors } = (await import(
  workflowInterceptorUrl
)) as typeof WorkflowContextInterceptorModule;
const {
  TEMPORAL_HEADER_DEADLINE_UNIX_NANO,
  TEMPORAL_HEADER_PRIORITY,
  currentTemporalActivityMessageContext,
  runWithTemporalSubmissionContext,
  temporalActivityInterceptors,
  temporalWorkflowClientInterceptor
} = propagation;

const payload = (value: string) => defaultPayloadConverter.toPayload(value);

function requiredHeader(headers: Headers, name: string) {
  const value = headers[name];
  assert.ok(value);
  return value;
}

await test("Temporal client interceptor injects the native context carrier", async () => {
  const deadlineBefore = Date.now() + 29_000;
  const context = new MessageContext()
    .withMetadata(
      new Map([
        ["traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"],
        ["tracestate", "vendor=value"],
        ["baggage", "tenant=test"]
      ])
    )
    .withStreamId("stream-1")
    .withPriority(-2)
    .bounded(30_000);
  let headers: Headers | undefined;
  const start = temporalWorkflowClientInterceptor.startWithDetails;
  assert.ok(start);
  await runWithTemporalSubmissionContext(context, () =>
    start({ headers: {}, workflowType: "workflow", options: {} } as WorkflowStartInput, (input) => {
      headers = input.headers;
      return Promise.resolve({ runId: "run", eagerlyStarted: false });
    })
  );
  assert.ok(headers);
  assert.equal(
    defaultPayloadConverter.fromPayload(requiredHeader(headers, "x-stream-id")),
    "stream-1"
  );
  assert.equal(
    defaultPayloadConverter.fromPayload(requiredHeader(headers, TEMPORAL_HEADER_PRIORITY)),
    "-2"
  );
  const deadlineNano = BigInt(
    defaultPayloadConverter.fromPayload<string>(
      requiredHeader(headers, TEMPORAL_HEADER_DEADLINE_UNIX_NANO)
    )
  );
  assert.ok(deadlineNano >= BigInt(deadlineBefore) * 1_000_000n);
  assert.ok(deadlineNano <= BigInt(Date.now() + 30_000) * 1_000_000n);
});

await test("Temporal Workflow forwards its native headers to the Activity", async () => {
  const registered = workflowInterceptors();
  const inbound = registered.inbound?.[0];
  const outbound = registered.outbound?.[0];
  assert.ok(inbound?.execute);
  assert.ok(outbound?.scheduleActivity);
  const carrier = { traceparent: payload("sampled-parent") };
  await inbound.execute({ args: [], headers: carrier }, () => Promise.resolve(undefined));
  let activityHeaders: Headers | undefined;
  await outbound.scheduleActivity(
    {
      activityType: "activity",
      args: [],
      headers: { baggage: payload("tenant=test") },
      options: { startToCloseTimeout: 1000 },
      seq: 1
    },
    (input) => {
      activityHeaders = input.headers;
      return Promise.resolve(undefined);
    }
  );
  assert.ok(activityHeaders);
  assert.equal(requiredHeader(activityHeaders, "traceparent"), carrier.traceparent);
  assert.ok(requiredHeader(activityHeaders, "baggage"));
});

await test("Temporal Activity interceptor restores MessageContext from UTC epoch headers", async () => {
  const deadlineUnixMillis = Date.now() + 30_000;
  const controller = new AbortController();
  const temporalContext = {
    cancellationSignal: controller.signal,
    info: { startToCloseTimeoutMs: 60_000 }
  } as TemporalActivityContext;
  const execute = temporalActivityInterceptors(temporalContext).inbound?.execute;
  assert.ok(execute);
  await execute(
    {
      args: [],
      headers: {
        traceparent: payload("00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"),
        baggage: payload("tenant=test"),
        "x-stream-id": payload("stream-2"),
        [TEMPORAL_HEADER_PRIORITY]: payload("4"),
        [TEMPORAL_HEADER_DEADLINE_UNIX_NANO]: payload(
          String(BigInt(deadlineUnixMillis) * 1_000_000n)
        )
      }
    },
    () => {
      const context = currentTemporalActivityMessageContext();
      assert.equal(context.streamId(), "stream-2");
      assert.equal(context.priority(), 4);
      assert.equal(context.metadata().get("baggage"), "tenant=test");
      assert.equal(context.samplingEnabled(), true);
      const remaining = context.remainingMs();
      assert.ok(remaining !== undefined && remaining > 28_000 && remaining <= 30_000);
      return Promise.resolve(undefined);
    }
  );
});
