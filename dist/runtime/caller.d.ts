import type { MessageContext } from "./context.js";
import type { PriorityTaskPoolLike, TaskPoolLike } from "./pool/pool.js";
import type { Caller, Consumer } from "./stream.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";
export type CallerRejectionHandler = (error: unknown) => void;
export declare class TaskPoolCaller<T> implements Caller<T> {
    #private;
    constructor(pool: TaskPoolLike, consumer: Consumer<T>, onRejected?: CallerRejectionHandler);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): void;
}
export declare class PriorityTaskPoolCaller<T> implements Caller<T> {
    #private;
    constructor(pool: PriorityTaskPoolLike, consumer: Consumer<T>, defaultPriority: number, onRejected?: CallerRejectionHandler);
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