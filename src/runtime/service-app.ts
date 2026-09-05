import {
  type CallSemanticsGroup,
  type CanonicalConfig,
  type RuntimeConfig,
  RuntimeConfigLoader,
  type RuntimeConfigReloadSource,
  type RuntimeConfigStore
} from "./config/index.js";
import { Context } from "./context.js";
import {
  err,
  type Logger,
  type LogsEngine,
  NoopLogsEngine,
  noopLogger,
  NoopMetricsEngine,
  PrometheusMetricsEngine,
  type Tracing,
  type TracingEngine
} from "./environment/index.js";
import { errorFromUnknown } from "./errors.js";
import { ServiceEnvironment, type JoinStorageFactory } from "./environment/runtime-environment.js";
import { RuntimeCallerFactory } from "./caller-factory.js";
import type { Lifecycle } from "./lifecycle.js";
import { JsonLogsEngine } from "./logging/index.js";
import { DelayPool, PriorityTaskPool, TaskPool } from "./pool/index.js";
import { ServiceRuntime } from "./service-runtime.js";
import { makeDefaultSerdeRegistry, type SerdeRegistry } from "./serde/index.js";
import { registerRuntimeHTTPHandlers } from "./status/index.js";
import type { CallerFactory } from "./stream.js";
import { RuntimeTaskRegistry } from "./task-registry.js";
import { RuntimeDiagnostics } from "./runtime-diagnostics.js";

export interface ServiceAppOptions<T extends CanonicalConfig = CanonicalConfig> {
  readonly callerFactory?: CallerFactory | undefined;
  readonly delayPool?: DelayPool | undefined;
  readonly serdeRegistry?: SerdeRegistry | undefined;
  readonly logger?: Logger | undefined;
  readonly logsEngine?: LogsEngine | undefined;
  readonly metricsEngine?: ServiceMetricsEngine | undefined;
  readonly tracing?: Tracing | undefined;
  readonly tracingEngine?: TracingEngine | undefined;
  readonly configReload?: RuntimeConfigReloadSource<T> | undefined;
  readonly joinStorageFactory?: JoinStorageFactory | undefined;
}

/** Owns one generated service runtime and its canonical component lifecycle. */
export class ServiceApp<T extends CanonicalConfig = CanonicalConfig> {
  readonly #environment: ServiceEnvironment<T>;
  readonly #runtime: ServiceRuntime;
  readonly #delayPool: DelayPool;
  readonly #metricsEngine: ServiceMetricsEngine;
  readonly #logsEngine: LogsEngine | undefined;
  readonly #tracingEngine: TracingEngine | undefined;
  readonly #runtimeDiagnostics: RuntimeDiagnostics;
  readonly #taskPools: readonly TaskPool[];
  readonly #priorityTaskPools: readonly PriorityTaskPool[];
  readonly #removeConfigValidation: () => void;
  readonly #unsubscribeConfig: () => void;
  #prepared = false;

  public constructor(
    config: RuntimeConfigStore<T>,
    serviceId: number,
    options: ServiceAppOptions<T> = {}
  ) {
    this.#metricsEngine = options.metricsEngine ?? makeServiceMetricsEngine();
    this.#logsEngine =
      options.logger === undefined ? (options.logsEngine ?? makeServiceLogsEngine()) : undefined;
    const logger = options.logger ?? this.#logsEngine?.defaultLogger() ?? noopLogger;
    const service = config.current().serviceById(serviceId);
    if (service === undefined) {
      throw new Error(`service config ${String(serviceId)} not found`);
    }
    this.#delayPool =
      options.delayPool ??
      new DelayPool({
        metrics: this.#metricsEngine.metrics(),
        service: service.name,
        logger,
        onError: reportAsyncFailure(logger, "delay pool task failed")
      });
    this.#tracingEngine = options.tracingEngine;
    const tracing = options.tracingEngine?.tracing() ?? options.tracing;
    const tasks = new RuntimeTaskRegistry(
      reportAsyncFailure(logger, "parallel runtime task failed")
    );
    const pools = makeRuntimePools(
      config.current(),
      this.#metricsEngine.metrics(),
      service.name,
      logger
    );
    this.#taskPools = [...pools.task.values()];
    this.#priorityTaskPools = [...pools.priority.values()];
    const callerFactory =
      options.callerFactory ??
      new RuntimeCallerFactory({
        config: () => config.current(),
        serviceId,
        taskPools: pools.task,
        priorityTaskPools: pools.priority,
        tasks
      });
    this.#environment = new ServiceEnvironment(
      config,
      serviceId,
      callerFactory,
      this.#delayPool,
      options.serdeRegistry ?? makeDefaultSerdeRegistry(),
      logger,
      this.#metricsEngine.metrics(),
      tracing,
      pools.task,
      pools.priority,
      options.joinStorageFactory
    );
    this.#removeConfigValidation = config.validate((next) => {
      validateRuntimePools(next, pools);
    });
    this.#unsubscribeConfig = config.subscribe((next) => {
      resizeRuntimePools(next, pools);
    });
    this.#runtime = new ServiceRuntime(this.#environment, tasks);
    this.#runtimeDiagnostics = new RuntimeDiagnostics(
      this.#metricsEngine.metrics(),
      service.name,
      tasks,
      this.#taskPools,
      this.#priorityTaskPools
    );
    if (options.configReload !== undefined) {
      this.#runtime.register({
        category: "component",
        name: "config-loader",
        lifecycle: new RuntimeConfigLoader({
          ...options.configReload,
          store: config,
          service: service.name,
          metrics: this.#metricsEngine.metrics(),
          logger
        })
      });
    }
  }

  public environment(): ServiceEnvironment<T> {
    return this.#environment;
  }

  public runtime(): ServiceRuntime {
    return this.#runtime;
  }

  public metricsEngine(): ServiceMetricsEngine {
    return this.#metricsEngine;
  }

  public start(context = Context.background()): Promise<void> {
    this.prepare();
    return this.#runtime.start(context);
  }

  public stop(context = Context.background(), drainTimeoutMs?: number): Promise<void> {
    return this.#runtime.stop(context, drainTimeoutMs).finally(() => {
      this.#removeConfigValidation();
      this.#unsubscribeConfig();
    });
  }

  private prepare(): void {
    if (this.#prepared) return;
    this.#prepared = true;
    registerRuntimeHTTPHandlers(this.#environment, this.#metricsEngine);
    for (const dataSource of this.#environment.dataSources()) {
      this.#runtime.register({
        category: "dataSource",
        name: dataSource.name,
        lifecycle: dataSource
      });
    }
    for (const dataSink of this.#environment.dataSinks()) {
      this.#runtime.register({ category: "dataSink", name: dataSink.name, lifecycle: dataSink });
    }
    for (const connector of this.#environment.managedDataConnectors()) {
      this.#runtime.register({
        category: "managedDataConnector",
        name: connector.name,
        lifecycle: connector
      });
    }
    for (const [index, storage] of this.#environment.storages().entries()) {
      this.#runtime.register({
        category: "storage",
        name: `storage-${String(index)}`,
        lifecycle: lifecycle(storage)
      });
    }
    for (const pool of this.#taskPools) {
      this.#runtime.register({ category: "taskPool", name: pool.name(), lifecycle: pool });
    }
    for (const pool of this.#priorityTaskPools) {
      this.#runtime.register({
        category: "priorityTaskPool",
        name: pool.name(),
        lifecycle: pool
      });
    }
    this.#runtime.register({ category: "delayPool", name: "delay", lifecycle: this.#delayPool });
    this.#runtime.register({
      category: "httpServer",
      name: "http",
      lifecycle: this.#environment.httpServer()
    });
    if (this.#logsEngine !== undefined) {
      this.#runtime.register({
        category: "telemetry",
        name: "logging",
        lifecycle: shutdownLifecycle((context) => this.#logsEngine?.shutdown(context))
      });
    }
    if (this.#tracingEngine !== undefined) {
      this.#runtime.register({
        category: "telemetry",
        name: "tracing",
        lifecycle: shutdownLifecycle((context) => this.#tracingEngine?.shutdown(context))
      });
    }
    this.#runtime.register({
      category: "telemetry",
      name: "metrics",
      lifecycle: shutdownLifecycle((context) => this.#metricsEngine.shutdown(context))
    });
    this.#runtime.register({
      category: "telemetry",
      name: "runtime-diagnostics",
      lifecycle: this.#runtimeDiagnostics
    });
  }
}

function makeRuntimePools(
  config: RuntimeConfig,
  metrics: ReturnType<ServiceMetricsEngine["metrics"]>,
  service: string,
  logger: Logger
): {
  readonly task: ReadonlyMap<string, TaskPool>;
  readonly priority: ReadonlyMap<string, PriorityTaskPool>;
} {
  const task = new Map<string, TaskPool>();
  const priority = new Map<string, PriorityTaskPool>();
  const use = (semantics: CallSemanticsGroup | undefined): void => {
    if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics) {
      return;
    }
    const poolName =
      "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
    const poolConfig = config.poolByName(poolName);
    if (poolConfig === undefined) throw new Error(`pool config ${poolName} not found`);
    if ("taskPool" in semantics) {
      if (!task.has(poolName)) {
        task.set(
          poolName,
          new TaskPool({
            name: poolName,
            executorsCount: poolConfig.executorsCount,
            metrics,
            service,
            logger,
            onError: reportAsyncFailure(logger, "task pool task failed")
          })
        );
      }
    } else if (!priority.has(poolName)) {
      priority.set(
        poolName,
        new PriorityTaskPool({
          name: poolName,
          executorsCount: poolConfig.executorsCount,
          metrics,
          service,
          logger,
          onError: reportAsyncFailure(logger, "priority task pool task failed")
        })
      );
    }
  };
  for (const link of config.config().links) use(link.callSemantics);
  for (const service of config.config().services) use(service.defaultCallSemantics);
  return { task, priority };
}

function reportAsyncFailure(logger: Logger, message: string): (error: unknown) => void {
  return (error: unknown): void => {
    logger.error(Context.background(), message, err(errorFromUnknown(error)));
  };
}

function resizeRuntimePools(
  config: RuntimeConfig,
  pools: {
    readonly task: ReadonlyMap<string, TaskPool>;
    readonly priority: ReadonlyMap<string, PriorityTaskPool>;
  }
): void {
  validateRuntimePools(config, pools);
  for (const [name, pool] of pools.task) {
    const poolConfig = config.poolByName(name);
    if (poolConfig === undefined) continue;
    pool.resize(poolConfig.executorsCount);
  }
  for (const [name, pool] of pools.priority) {
    const poolConfig = config.poolByName(name);
    if (poolConfig === undefined) continue;
    pool.resize(poolConfig.executorsCount);
  }
}

function validateRuntimePools(
  config: RuntimeConfig,
  pools: {
    readonly task: ReadonlyMap<string, TaskPool>;
    readonly priority: ReadonlyMap<string, PriorityTaskPool>;
  }
): void {
  for (const name of [...pools.task.keys(), ...pools.priority.keys()]) {
    if (config.poolByName(name) === undefined) throw new Error(`pool config ${name} not found`);
  }
}

export type ServiceMetricsEngine = (PrometheusMetricsEngine | NoopMetricsEngine) & {
  contentType(): string;
  render(): Promise<string>;
};

export function environmentFlagEnabled(
  name: string,
  environment: NodeJS.ProcessEnv = process.env
): boolean {
  const value = environment[name]?.trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

export function makeServiceMetricsEngine(
  environment: NodeJS.ProcessEnv = process.env
): ServiceMetricsEngine {
  return environmentFlagEnabled("SERVICELIB_NOOP_METRICS", environment)
    ? new NoopMetricsEngine()
    : new PrometheusMetricsEngine();
}

export function makeServiceLogsEngine(environment: NodeJS.ProcessEnv = process.env): LogsEngine {
  return environmentFlagEnabled("SERVICELIB_NOOP_LOGS", environment)
    ? new NoopLogsEngine()
    : new JsonLogsEngine();
}

function lifecycle(value: {
  start(context: Context): void | Promise<void>;
  stop(context: Context): void | Promise<void>;
}): Lifecycle {
  return {
    async start(context): Promise<void> {
      await value.start(context);
    },
    async stop(context): Promise<void> {
      await value.stop(context);
    }
  };
}

function shutdownLifecycle(shutdown: (context: Context) => void | Promise<void>): Lifecycle {
  return {
    start(): Promise<void> {
      return Promise.resolve();
    },
    async stop(context): Promise<void> {
      await shutdown(context);
    }
  };
}
