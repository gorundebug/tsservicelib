import assert from "node:assert/strict";
import test from "node:test";

import {
  ScheduleBackend,
  makeDefaultSerdeRegistry,
  makeScheduleTrigger,
  normalizeTemporalPriority,
  scheduleTriggerSerdeType
} from "@gorundebug/tsservicelib/runtime";

await test("schedule trigger identity is stable across retries", () => {
  const scheduledAt = "2026-08-24T12:30:00.123456Z";
  const first = makeScheduleTrigger(
    17,
    "hourly",
    scheduledAt,
    "2026-08-24T12:30:00.223456Z",
    ScheduleBackend.Temporal
  );
  const retry = makeScheduleTrigger(
    17,
    "hourly",
    scheduledAt,
    "2026-08-24T12:30:01.223456Z",
    ScheduleBackend.Temporal
  );

  assert.equal(first.triggerId, retry.triggerId);
  assert.equal(first.triggerId, "29b272e3eeee0c67fe5b5a121f8f39d4b5d9625d656e8a0ec7f2b0f1615e2914");
  assert.notEqual(first.firedAt, retry.firedAt);
  assert(Object.isFrozen(first));
});

await test("schedule trigger uses the built-in typed JSON serde", () => {
  const trigger = makeScheduleTrigger(
    17,
    "hourly",
    "2026-08-24T12:30:00.123456Z",
    "2026-08-24T12:30:00.223456Z",
    ScheduleBackend.Local
  );
  const registry = makeDefaultSerdeRegistry();
  const serde = registry.require(scheduleTriggerSerdeType);
  assert.deepEqual(serde.deserialize(serde.serialize(trigger)), trigger);
  assert.equal(registry.requireByName("ScheduleTrigger"), serde);
});

await test("Temporal priority normalization is bounded and monotonic", () => {
  assert.deepEqual(
    [-100, -2, -1, 0, 1, 2, 100].map(normalizeTemporalPriority),
    [1, 1, 2, 3, 4, 5, 5]
  );
});
