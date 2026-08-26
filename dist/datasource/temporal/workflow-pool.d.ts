import type { Context } from "../../runtime/context.js";
import type { Metrics } from "../../runtime/environment/metrics/metrics.js";
import { type PoolTask, type PriorityTaskPoolLike, type TaskPoolLike } from "../../runtime/pool/pool.js";
export declare class WorkflowTaskPool implements TaskPoolLike {
    #private;
    constructor(name: string, executors: number, metrics: Metrics, service: string, onError: (error: unknown) => void);
    name(): string;
    executorsCount(): number;
    queueLength(): number;
    activeCount(): number;
    start(): void;
    stop(): Promise<void>;
    waitIdle(): Promise<void>;
    addTask(context: Context, execute: PoolTask): void;
}
export declare class WorkflowPriorityTaskPool implements PriorityTaskPoolLike {
    #private;
    constructor(name: string, executors: number, metrics: Metrics, service: string, onError: (error: unknown) => void);
    name(): string;
    executorsCount(): number;
    queueLength(): number;
    activeCount(): number;
    start(): void;
    stop(): Promise<void>;
    waitIdle(): Promise<void>;
    addTask(context: Context, priority: number, execute: PoolTask): void;
}
//# sourceMappingURL=workflow-pool.d.ts.map