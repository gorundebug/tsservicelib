import type { Context } from "../context.js";
import type { Lifecycle } from "../lifecycle.js";
import { type PoolTask, type TaskPoolOptions } from "./pool.js";
/** Minimum-priority-first task pool with FIFO ordering for equal priorities. */
export declare class PriorityTaskPool implements Lifecycle {
    #private;
    constructor(options: TaskPoolOptions);
    name(): string;
    executorsCount(): number;
    queueLength(): number;
    activeCount(): number;
    resize(executorsCount: number): void;
    start(context: Context): Promise<void>;
    addTask(context: Context, priority: number, execute: PoolTask): void;
    stop(context: Context): Promise<void>;
    private insert;
    private pump;
    private run;
    private taskFinished;
    private finishDrainIfIdle;
}
//# sourceMappingURL=priority-task-pool.d.ts.map