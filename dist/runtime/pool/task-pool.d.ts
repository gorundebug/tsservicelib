import type { Context } from "../context.js";
import type { Lifecycle } from "../lifecycle.js";
import { type PoolTask, type TaskPoolOptions } from "./pool.js";
/** Canonical unbounded FIFO task pool. Queue capacity is not backpressure. */
export declare class TaskPool implements Lifecycle {
    #private;
    constructor(options: TaskPoolOptions);
    name(): string;
    executorsCount(): number;
    queueLength(): number;
    activeCount(): number;
    resize(executorsCount: number): void;
    start(context: Context): Promise<void>;
    addTask(context: Context, execute: PoolTask): void;
    stop(context: Context): Promise<void>;
    private pump;
    private run;
    private taskFinished;
    private finishDrainIfIdle;
}
//# sourceMappingURL=task-pool.d.ts.map