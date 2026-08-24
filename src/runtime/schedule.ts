import { createHash } from "node:crypto";

import type { Collector } from "./collector.js";
import type { MessageContext } from "./context.js";
import type { Completion } from "./stream.js";

export const ScheduleBackend = {
  Local: "local",
  Temporal: "temporal"
} as const;

export type ScheduleBackend = (typeof ScheduleBackend)[keyof typeof ScheduleBackend];

/** Portable payload emitted by Cron and Temporal schedule data sources. */
export interface ScheduleTrigger {
  readonly triggerId: string;
  readonly scheduleId: string;
  readonly scheduledAt: string;
  readonly firedAt: string;
  readonly backend: ScheduleBackend;
}

/** User-defined conversion boundary between a schedule and its graph input. */
export interface ScheduleEndpointFunction<T> {
  onTrigger(
    context: MessageContext,
    trigger: Readonly<ScheduleTrigger>,
    out: Collector<T>
  ): Completion;
}

export function isScheduleTrigger(value: unknown): value is ScheduleTrigger {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const candidate = value as Readonly<Record<string, unknown>>;
  return (
    typeof candidate["triggerId"] === "string" &&
    /^[0-9a-f]{64}$/u.test(candidate["triggerId"]) &&
    typeof candidate["scheduleId"] === "string" &&
    typeof candidate["scheduledAt"] === "string" &&
    typeof candidate["firedAt"] === "string" &&
    (candidate["backend"] === ScheduleBackend.Local ||
      candidate["backend"] === ScheduleBackend.Temporal)
  );
}

function requireUtcTimestamp(value: string, field: string): void {
  if (!value.endsWith("Z") || !Number.isFinite(Date.parse(value))) {
    throw new TypeError(`${field} must be an RFC3339 UTC timestamp`);
  }
}

/**
 * Constructs the cross-language trigger identity. Timestamps intentionally stay
 * as RFC3339 strings so sub-millisecond precision survives durable serialization.
 */
export function makeScheduleTrigger(
  endpointId: number,
  scheduleId: string,
  scheduledAt: string,
  firedAt: string,
  backend: ScheduleBackend
): ScheduleTrigger {
  if (!Number.isSafeInteger(endpointId) || endpointId < 1) {
    throw new RangeError("endpointId must be a positive safe integer");
  }
  if (scheduleId.length === 0) throw new TypeError("scheduleId must not be empty");
  requireUtcTimestamp(scheduledAt, "scheduledAt");
  requireUtcTimestamp(firedAt, "firedAt");
  const identity = `servicegen:schedule-trigger:v1\n${String(endpointId)}\n${scheduleId}\n${scheduledAt}`;
  return Object.freeze({
    triggerId: createHash("sha256").update(identity).digest("hex"),
    scheduleId,
    scheduledAt,
    firedAt,
    backend
  });
}

/** Maps the graph priority range to Temporal's portable 1..5 priority scale. */
export function normalizeTemporalPriority(priority: number): number {
  if (!Number.isFinite(priority)) throw new TypeError("priority must be finite");
  if (priority <= -2) return 1;
  if (priority === -1) return 2;
  if (priority === 0) return 3;
  if (priority === 1) return 4;
  return 5;
}
