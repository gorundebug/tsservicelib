import type { MessageContext } from "./context.js";
import type { PriorityTaskPool } from "./pool/priority-task-pool.js";
import type { TaskPool } from "./pool/task-pool.js";
import type { Caller, Consumer } from "./stream.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";
export type CallerRejectionHandler = (error: unknown) => void;
export declare class TaskPoolCaller<T> implements Caller<T> {
    #private;
    constructor(pool: TaskPool, consumer: Consumer<T>, onRejected?: CallerRejectionHandler);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): void;
}
export declare class PriorityTaskPoolCaller<T> implements Caller<T> {
    #private;
    constructor(pool: PriorityTaskPool, consumer: Consumer<T>, defaultPriority: number, onRejected?: CallerRejectionHandler);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): void;
}
export declare class ParallelCaller<T> implements Caller<T> {
    #private;
    constructor(tasks: RuntimeTaskRegistry, consumer: Consumer<T>);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): void;
}
//# sourceMappingURL=caller.d.ts.map