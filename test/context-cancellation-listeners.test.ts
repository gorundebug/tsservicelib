import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { test } from "node:test";

import { combineAbortSignals } from "@gorundebug/tsservicelib/runtime";

await test("a pre-cancelled constituent leaves no listeners on other contexts", () => {
  const active = new AbortController();
  const cancelled = new AbortController();
  const reason = new Error("already cancelled");
  cancelled.abort(reason);
  const before = getEventListeners(active.signal, "abort").length;
  const combined = combineAbortSignals([active.signal, cancelled.signal]);
  assert.equal(combined.aborted, true);
  assert.equal(combined.reason, reason);
  assert.equal(getEventListeners(active.signal, "abort").length, before);
});

await test("repeated context signals create at most one subscription and clean up on abort", () => {
  const parent = new AbortController();
  const combined = combineAbortSignals([parent.signal, parent.signal]);
  try {
    assert.ok(getEventListeners(parent.signal, "abort").length <= 1);
  } finally {
    parent.abort(new Error("finished"));
  }
  assert.equal(combined.aborted, true);
  assert.equal(getEventListeners(parent.signal, "abort").length, 0);
});

await test("native cancellation composition keeps live descendants and parent reason without listener retention", () => {
  const parent = new AbortController(), generation = new AbortController();
  let retained: AbortSignal | undefined;
  for (let i = 0; i < 4096; i++) retained = combineAbortSignals([parent.signal, generation.signal]);
  assert.equal(getEventListeners(parent.signal, "abort").length, 0);
  assert.equal(getEventListeners(generation.signal, "abort").length, 0);
  const reason = new Error("assignment revoked");
  generation.abort(reason);
  assert.equal(retained?.aborted, true);
  assert.equal(retained.reason, reason);
  assert.equal(parent.signal.aborted, false);
});

await test("portable cancellation fallback cleans completed generation bindings", (t) => {
  const descriptor = Object.getOwnPropertyDescriptor(AbortSignal, "any");
  assert.ok(descriptor);
  Object.defineProperty(AbortSignal, "any", { ...descriptor, value: undefined });
  t.after(() => Object.defineProperty(AbortSignal, "any", descriptor));
  const parent = new AbortController(), generation = new AbortController();
  const joined = combineAbortSignals([parent.signal, generation.signal]);
  assert.equal(getEventListeners(parent.signal, "abort").length, 1);
  const reason = new Error("generation stopped");
  generation.abort(reason);
  assert.equal(joined.reason, reason);
  assert.equal(getEventListeners(parent.signal, "abort").length, 0);
});
