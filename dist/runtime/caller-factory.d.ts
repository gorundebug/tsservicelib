import type { RuntimeConfig } from "./config/index.js";
import { type CallerRejectionHandler } from "./caller.js";
import type { PriorityTaskPool, TaskPool } from "./pool/index.js";
import { type Caller, type CallerFactory, type Stream, type TypedStreamConsumer } from "./stream.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";
import { type DurableTransport } from "./durable.js";
export interface RuntimeCallerFactoryOptions {
    readonly config: () => RuntimeConfig;
    readonly serviceId: number;
    readonly taskPools: ReadonlyMap<string, TaskPool>;
    readonly priorityTaskPools: ReadonlyMap<string, PriorityTaskPool>;
    readonly tasks: RuntimeTaskRegistry;
    readonly durableTransport?: ((id: number) => DurableTransport | undefined) | undefined;
    readonly onRejected?: CallerRejectionHandler | undefined;
}
/** Resolves the immutable graph link semantics once when a caller is built. */
export declare class RuntimeCallerFactory implements CallerFactory {
    #private;
    constructor(options: RuntimeCallerFactoryOptions);
    create<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T>;
}
//# sourceMappingURL=caller-factory.d.ts.map