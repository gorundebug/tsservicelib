import type { RuntimeConfig } from "./config/index.js";
import { setCallerMetadata } from "./caller-metadata.js";
import {
  ParallelCaller,
  PriorityTaskPoolCaller,
  TaskPoolCaller,
  type CallerRejectionHandler
} from "./caller.js";
import type { PriorityTaskPool, TaskPool } from "./pool/index.js";
import {
  FunctionCaller,
  type Caller,
  type CallerFactory,
  type Stream,
  type TypedStream,
  type TypedStreamConsumer
} from "./stream.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";
import { DurableCaller, makeDurableLinkHandler, type DurableTransport } from "./durable.js";

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
export class RuntimeCallerFactory implements CallerFactory {
  readonly #options: RuntimeCallerFactoryOptions;

  public constructor(options: RuntimeCallerFactoryOptions) {
    this.#options = options;
  }

  public create<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T> {
    const config = this.#options.config();
    const service = config.serviceById(this.#options.serviceId);
    if (service === undefined) {
      throw new Error(`service config ${String(this.#options.serviceId)} not found`);
    }
    const semantics =
      config.link(source.id, consumer.id)?.callSemantics ??
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
      return setCallerMetadata(
        new PriorityTaskPoolCaller(
          pool,
          consumer,
          semantics.priorityTaskPool.priority,
          this.#options.onRejected
        ),
        { type: "prioritytaskpool", taskPoolName: pool.name() }
      );
    }
    if ("durableCall" in semantics) {
      const transport = this.#options.durableTransport?.(semantics.durableCall.idDataConnector);
      if (transport === undefined) {
        throw new Error(
          `durable caller for Temporal connector ${String(semantics.durableCall.idDataConnector)} is not registered`
        );
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

function isTypedStream<T>(stream: Stream): stream is TypedStream<T> {
  return "serde" in stream && typeof stream.serde === "function";
}

const DEFAULT_CALL_SEMANTICS = { functionCall: { async: false } } as const;
