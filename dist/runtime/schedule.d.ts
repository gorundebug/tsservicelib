import type { Collector } from "./collector.js";
import type { MessageContext } from "./context.js";
import type { Completion } from "./stream.js";
export declare const ScheduleBackend: {
    readonly Local: "local";
    readonly Temporal: "temporal";
};
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
    onTrigger(context: MessageContext, trigger: Readonly<ScheduleTrigger>, out: Collector<T>): Completion;
}
export declare function isScheduleTrigger(value: unknown): value is ScheduleTrigger;
/**
 * Constructs the cross-language trigger identity. Timestamps intentionally stay
 * as RFC3339 strings so sub-millisecond precision survives durable serialization.
 */
export declare function makeScheduleTrigger(endpointId: number, scheduleId: string, scheduledAt: string, firedAt: string, backend: ScheduleBackend): ScheduleTrigger;
/** Maps the graph priority range to Temporal's portable 1..5 priority scale. */
export declare function normalizeTemporalPriority(priority: number): number;
//# sourceMappingURL=schedule.d.ts.map