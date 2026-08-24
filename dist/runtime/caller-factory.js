import { setCallerMetadata } from "./caller-metadata.js";
import { ParallelCaller, PriorityTaskPoolCaller, TaskPoolCaller } from "./caller.js";
import { FunctionCaller } from "./stream.js";
import { DurableCaller, makeDurableLinkHandler } from "./durable.js";
/** Resolves the immutable graph link semantics once when a caller is built. */
export class RuntimeCallerFactory {
    #options;
    constructor(options) {
        this.#options = options;
    }
    create(source, consumer) {
        const config = this.#options.config();
        const service = config.serviceById(this.#options.serviceId);
        if (service === undefined) {
            throw new Error(`service config ${String(this.#options.serviceId)} not found`);
        }
        const semantics = config.link(source.id, consumer.id)?.callSemantics ??
            service.defaultCallSemantics ??
            DEFAULT_CALL_SEMANTICS;
        if ("functionCall" in semantics) {
            return new FunctionCaller(consumer, semantics.functionCall.async);
        }
        if ("taskPool" in semantics) {
            const pool = this.#options.taskPools.get(semantics.taskPool.poolName);
            if (pool === undefined) {
                throw new Error(`task pool ${semantics.taskPool.poolName} not found`);
            }
            return setCallerMetadata(new TaskPoolCaller(pool, consumer, this.#options.onRejected), {
                type: "taskpool",
                taskPoolName: pool.name()
            });
        }
        if ("priorityTaskPool" in semantics) {
            const pool = this.#options.priorityTaskPools.get(semantics.priorityTaskPool.poolName);
            if (pool === undefined) {
                throw new Error(`priority task pool ${semantics.priorityTaskPool.poolName} not found`);
            }
            return setCallerMetadata(new PriorityTaskPoolCaller(pool, consumer, semantics.priorityTaskPool.priority, this.#options.onRejected), { type: "prioritytaskpool", taskPoolName: pool.name() });
        }
        if ("durableCall" in semantics) {
            const transport = this.#options.durableTransport?.(semantics.durableCall.idDataConnector);
            if (transport === undefined) {
                throw new Error(`durable caller for Temporal connector ${String(semantics.durableCall.idDataConnector)} is not registered`);
            }
            if (!isTypedStream(source)) {
                throw new Error(`durable source stream ${source.name} has no serde`);
            }
            const link = { from: source.id, to: consumer.id };
            transport.registerLink(link, makeDurableLinkHandler(consumer, source.serde()));
            return setCallerMetadata(new DurableCaller(link, transport, source.serde()), {
                type: "durable"
            });
        }
        return setCallerMetadata(new ParallelCaller(this.#options.tasks, consumer), {
            type: "parallel"
        });
    }
}
function isTypedStream(stream) {
    return "serde" in stream && typeof stream.serde === "function";
}
const DEFAULT_CALL_SEMANTICS = { functionCall: { async: false } };
//# sourceMappingURL=caller-factory.js.map