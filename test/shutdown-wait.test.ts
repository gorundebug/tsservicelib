import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { test } from "node:test";

import { Context, settleWithinDeadline } from "@gorundebug/tsservicelib/runtime";

await test("shutdown wait preserves completed values and failures and removes its listener", async () => {
  const context = Context.background().bounded(1_000);
  const before = getEventListeners(context.signal(), "abort").length;
  const failure = new Error("stop failed");
  const results = await settleWithinDeadline(context, [
    Promise.resolve(42),
    Promise.reject(failure)
  ]);
  assert.deepEqual(results, [
    { status: "fulfilled", value: 42 },
    { status: "rejected", reason: failure }
  ]);
  assert.equal(getEventListeners(context.signal(), "abort").length, before);
});

await test("shutdown wait observes cancellation without a deadline and handles late rejection", async () => {
  const cancellation = new AbortController();
  const context = new Context(cancellation.signal);
  let fail: (reason: Error) => void = () => undefined;
  const pending = new Promise<never>((_resolve, reject) => {
    fail = reject;
  });
  const waiting = settleWithinDeadline(context, [pending]);
  cancellation.abort(new Error("stop cancelled"));
  const results = await waiting;
  assert.deepEqual(results, [undefined]);
  assert.equal(getEventListeners(context.signal(), "abort").length, 0);
  fail(new Error("late stop failure"));
  await Promise.resolve();
  assert.deepEqual(
    results,
    [undefined],
    "returned results are a snapshot, not a mutable completion list"
  );
});

await test("shutdown wait deadline does not wait for an uncooperative operation", async () => {
  const context = Context.background().bounded(10);
  let release: () => void = () => undefined;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  try {
    const results = await settleWithinDeadline(context, [pending]);
    assert.deepEqual(results, [undefined]);
    assert.equal(getEventListeners(context.signal(), "abort").length, 0);
  } finally {
    release();
  }
});
