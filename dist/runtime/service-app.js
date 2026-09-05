import { RuntimeConfigLoader } from "./config/index.js";
import { Context } from "./context.js";
import { err, NoopLogsEngine, noopLogger, NoopMetricsEngine, PrometheusMetricsEngine } from "./environment/index.js";
import { errorFromUnknown } from "./errors.js";
import { ServiceEnvironment } from "./environment/runtime-environment.js";
import { RuntimeCallerFactory } from "./caller-factory.js";
import { JsonLogsEngine } from "./logging/index.js";
import { DelayPool, PriorityTaskPool, TaskPool } from "./pool/index.js";
import { ServiceRuntime } from "./service-runtime.js";
import { makeDefaultSerdeRegistry } from "./serde/index.js";
import { registerRuntimeHTTPHandlers } from "./status/index.js";
import { RuntimeTaskRegistry } from "./task-registry.js";
import { RuntimeDiagnostics } from "./runtime-diagnostics.js";
/** Owns one generated service runtime and its canonical component lifecycle. */
export class ServiceApp {
    #environment;
    #runtime;
    #delayPool;
    #metricsEngine;
    #logsEngine;
    #tracingEngine;
    #runtimeDiagnostics;
    #taskPools;
    #priorityTaskPools;
    #removeConfigValidation;
    #unsubscribeConfig;
    #prepared = false;
    constructor(config, serviceId, options = {}) {
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
        const tasks = new RuntimeTaskRegistry(reportAsyncFailure(logger, "parallel runtime task failed"));
        const pools = makeRuntimePools(config.current(), this.#metricsEngine.metrics(), service.name, logger);
        this.#taskPools = [...pools.task.values()];
        this.#priorityTaskPools = [...pools.priority.values()];
        const callerFactory = options.callerFactory ??
            new RuntimeCallerFactory({
                config: () => config.current(),
                serviceId,
                taskPools: pools.task,
                priorityTaskPools: pools.priority,
                tasks
            });
        this.#environment = new ServiceEnvironment(config, serviceId, callerFactory, this.#delayPool, options.serdeRegistry ?? makeDefaultSerdeRegistry(), logger, this.#metricsEngine.metrics(), tracing, pools.task, pools.priority, options.joinStorageFactory);
        this.#removeConfigValidation = config.validate((next) => {
            validateRuntimePools(next, pools);
        });
        this.#unsubscribeConfig = config.subscribe((next) => {
            resizeRuntimePools(next, pools);
        });
        this.#runtime = new ServiceRuntime(this.#environment, tasks);
        this.#runtimeDiagnostics = new RuntimeDiagnostics(this.#metricsEngine.metrics(), service.name, tasks, this.#taskPools, this.#priorityTaskPools);
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
    environment() {
        return this.#environment;
    }
    runtime() {
        return this.#runtime;
    }
    metricsEngine() {
        return this.#metricsEngine;
    }
    start(context = Context.background()) {
        this.prepare();
        return this.#runtime.start(context);
    }
    stop(context = Context.background(), drainTimeoutMs) {
        return this.#runtime.stop(context, drainTimeoutMs).finally(() => {
            this.#removeConfigValidation();
            this.#unsubscribeConfig();
        });
    }
    prepare() {
        if (this.#prepared)
            return;
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
function makeRuntimePools(config, metrics, service, logger) {
    const task = new Map();
    const priority = new Map();
    const use = (semantics) => {
        if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics) {
            return;
        }
        const poolName = "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
        const poolConfig = config.poolByName(poolName);
        if (poolConfig === undefined)
            throw new Error(`pool config ${poolName} not found`);
        if ("taskPool" in semantics) {
            if (!task.has(poolName)) {
                task.set(poolName, new TaskPool({
                    name: poolName,
                    executorsCount: poolConfig.executorsCount,
                    metrics,
                    service,
                    logger,
                    onError: reportAsyncFailure(logger, "task pool task failed")
                }));
            }
        }
        else if (!priority.has(poolName)) {
            priority.set(poolName, new PriorityTaskPool({
                name: poolName,
                executorsCount: poolConfig.executorsCount,
                metrics,
                service,
                logger,
                onError: reportAsyncFailure(logger, "priority task pool task failed")
            }));
        }
    };
    for (const link of config.config().links)
        use(link.callSemantics);
    for (const service of config.config().services)
        use(service.defaultCallSemantics);
    return { task, priority };
}
function reportAsyncFailure(logger, message) {
    return (error) => {
        logger.error(Context.background(), message, err(errorFromUnknown(error)));
    };
}
function resizeRuntimePools(config, pools) {
    validateRuntimePools(config, pools);
    for (const [name, pool] of pools.task) {
        const poolConfig = config.poolByName(name);
        if (poolConfig === undefined)
            continue;
        pool.resize(poolConfig.executorsCount);
    }
    for (const [name, pool] of pools.priority) {
        const poolConfig = config.poolByName(name);
        if (poolConfig === undefined)
            continue;
        pool.resize(poolConfig.executorsCount);
    }
}
function validateRuntimePools(config, pools) {
    for (const name of [...pools.task.keys(), ...pools.priority.keys()]) {
        if (config.poolByName(name) === undefined)
            throw new Error(`pool config ${name} not found`);
    }
}
export function environmentFlagEnabled(name, environment = process.env) {
    const value = environment[name]?.trim().toLowerCase();
    return value === "1" || value === "true" || value === "yes" || value === "on";
}
export function makeServiceMetricsEngine(environment = process.env) {
    return environmentFlagEnabled("SERVICELIB_NOOP_METRICS", environment)
        ? new NoopMetricsEngine()
        : new PrometheusMetricsEngine();
}
export function makeServiceLogsEngine(environment = process.env) {
    return environmentFlagEnabled("SERVICELIB_NOOP_LOGS", environment)
        ? new NoopLogsEngine()
        : new JsonLogsEngine();
}
function lifecycle(value) {
    return {
        async start(context) {
            await value.start(context);
        },
        async stop(context) {
            await value.stop(context);
        }
    };
}
function shutdownLifecycle(shutdown) {
    return {
        start() {
            return Promise.resolve();
        },
        async stop(context) {
            await shutdown(context);
        }
    };
}
//# sourceMappingURL=service-app.js.map