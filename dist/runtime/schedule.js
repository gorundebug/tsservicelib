import { createHash } from "node:crypto";
export const ScheduleBackend = {
    Local: "local",
    Temporal: "temporal"
};
export function isScheduleTrigger(value) {
    if (typeof value !== "object" || value === null || Array.isArray(value))
        return false;
    const candidate = value;
    return (typeof candidate["triggerId"] === "string" &&
        /^[0-9a-f]{64}$/u.test(candidate["triggerId"]) &&
        typeof candidate["scheduleId"] === "string" &&
        typeof candidate["scheduledAt"] === "string" &&
        typeof candidate["firedAt"] === "string" &&
        (candidate["backend"] === ScheduleBackend.Local ||
            candidate["backend"] === ScheduleBackend.Temporal));
}
function requireUtcTimestamp(value, field) {
    if (!value.endsWith("Z") || !Number.isFinite(Date.parse(value))) {
        throw new TypeError(`${field} must be an RFC3339 UTC timestamp`);
    }
}
/**
 * Constructs the cross-language trigger identity. Timestamps intentionally stay
 * as RFC3339 strings so sub-millisecond precision survives durable serialization.
 */
export function makeScheduleTrigger(endpointId, scheduleId, scheduledAt, firedAt, backend) {
    if (!Number.isSafeInteger(endpointId) || endpointId < 1) {
        throw new RangeError("endpointId must be a positive safe integer");
    }
    if (scheduleId.length === 0)
        throw new TypeError("scheduleId must not be empty");
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
export function normalizeTemporalPriority(priority) {
    if (!Number.isFinite(priority))
        throw new TypeError("priority must be finite");
    if (priority <= -2)
        return 1;
    if (priority === -1)
        return 2;
    if (priority === 0)
        return 3;
    if (priority === 1)
        return 4;
    return 5;
}
//# sourceMappingURL=schedule.js.map