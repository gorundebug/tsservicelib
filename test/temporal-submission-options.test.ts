import assert from "node:assert/strict";
import test from "node:test";

import { resolveSubmissionQueue } from "@gorundebug/tsservicelib/datasource/temporal";

await test("submission options preserve configured defaults and select explicit queues", () => {
  assert.equal(resolveSubmissionQueue("configured", "Activity", {}), "configured");
  assert.equal(
    resolveSubmissionQueue("configured", "Activity", {
      taskQueue: "selected",
      scheduleToStartTimeout: 2000
    }),
    "selected"
  );
  assert.equal(
    resolveSubmissionQueue("configured", "Workflow", { taskQueue: "selected" }),
    "selected"
  );
});

await test("submission options reject invalid explicit values rather than falling back", () => {
  for (const taskQueue of ["  ", "queue\0"]) {
    assert.throws(() => resolveSubmissionQueue("configured", "Activity", { taskQueue }));
  }
  for (const scheduleToStartTimeout of [-1, NaN, Infinity]) {
    assert.throws(() =>
      resolveSubmissionQueue("configured", "Activity", { scheduleToStartTimeout })
    );
  }
  assert.throws(() =>
    resolveSubmissionQueue("configured", "Workflow", { scheduleToStartTimeout: 1 })
  );
});
