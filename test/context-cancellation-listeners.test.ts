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
