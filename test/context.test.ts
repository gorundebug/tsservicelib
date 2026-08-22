import assert from "node:assert/strict";
import { test } from "node:test";

import {
  MessageContext,
  STREAM_ID_HEADER,
  TRACE_SAMPLING_HEADER
} from "@gorundebug/tsservicelib/runtime";

await test("MessageContext preserves explicit priority zero and immutable stream metadata", () => {
  const original = new MessageContext();
  const updated = original.withPriority(0).withStreamId("request-1");

  assert.equal(original.priority(), undefined);
  assert.equal(original.streamId(), undefined);
  assert.equal(updated.priority(), 0);
  assert.equal(updated.streamId(), "request-1");
  assert.equal(updated.transportMetadata().get(STREAM_ID_HEADER), "request-1");
});

await test("MessageContext derives sampling only from explicit or sampled transport metadata", () => {
  const unsampled = new MessageContext().withMetadata(
    new Map([["traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"]])
  );
  const sampled = unsampled.withMetadata(
    new Map([["traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"]])
  );
  const explicit = new MessageContext().withSampling(true);

  assert.equal(unsampled.samplingEnabled(), false);
  assert.equal(sampled.samplingEnabled(), true);
  assert.equal(explicit.transportMetadata().get(TRACE_SAMPLING_HEADER), "1");
});

await test("bounded context keeps the earlier deadline and observes later cancellation", async () => {
  const controller = new AbortController();
  const original = new MessageContext().bounded(100).withExternalCancellation(controller.signal);
  const bounded = original.bounded(1_000);

  assert.ok((bounded.remainingMs() ?? Infinity) <= 100);
  assert.equal(bounded.cancelled(), false);
  controller.abort(new Error("cancelled"));
  await Promise.resolve();
  assert.equal(bounded.cancelled(), true);
});

await test("child contexts report the same earliest deadline that controls cancellation", () => {
  const parent = new MessageContext().bounded(100);
  const parentDeadline = parent.deadline();
  assert.ok(parentDeadline !== undefined);

  const later = parent.withDeadline(parentDeadline + 1_000);
  const unspecified = parent.withDeadline(undefined);
  const earlier = parent.withDeadline(parentDeadline - 10);

  assert.equal(later, parent);
  assert.equal(unspecified, parent);
  assert.equal(later.deadline(), parentDeadline);
  assert.equal(unspecified.deadline(), parentDeadline);
  assert.equal(earlier.deadline(), parentDeadline - 10);
});

await test("reusing the same cancellation signal does not build a redundant composite", () => {
  const controller = new AbortController();
  const context = new MessageContext(controller.signal);

  assert.equal(context.withExternalCancellation(controller.signal), context);
});

await test("detached child drops cancellation and deadline but preserves message values", async () => {
  const controller = new AbortController();
  const parent = new MessageContext()
    .withStreamId("detached-request")
    .withPriority(0)
    .withSampling(true)
    .bounded(100)
    .withExternalCancellation(controller.signal);
  const detached = parent.withoutCancellation();

  controller.abort(new Error("parent cancelled"));
  await Promise.resolve();

  assert.equal(parent.cancelled(), true);
  assert.equal(detached.cancelled(), false);
  assert.equal(detached.deadline(), undefined);
  assert.equal(detached.streamId(), "detached-request");
  assert.equal(detached.priority(), 0);
  assert.equal(detached.samplingEnabled(), true);
  assert.equal(detached.transportMetadata().get(TRACE_SAMPLING_HEADER), "1");
});
