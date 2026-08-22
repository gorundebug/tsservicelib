import type { Context } from "../context.js";
import type { Float64Histogram, Int64Counter, Int64Gauge } from "../environment/index.js";
import type { TaskPoolOptions } from "./pool.js";
export interface TaskPoolMetrics {
    readonly queueLength: Int64Gauge;
    readonly executorsTarget: Int64Gauge;
    readonly executorsAllocated: Int64Gauge;
    readonly executorsBusy: Int64Gauge;
    readonly tasksTotal: Int64Counter;
    readonly executionDuration: Float64Histogram;
    readonly stopTimeout: Int64Counter;
    readonly taskRejected: Int64Counter;
    readonly taskCancelledOrExpired: Int64Counter;
}
export declare function makeTaskPoolMetrics(kind: "task" | "priority", options: TaskPoolOptions): TaskPoolMetrics | undefined;
export declare function awaitPoolDrain(drain: Promise<void>, context: Context, onTimeout: () => void): Promise<void>;
//# sourceMappingURL=task-pool-metrics.d.ts.map