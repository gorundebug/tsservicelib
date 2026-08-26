import { RuntimeConfig } from "../../runtime/config/runtime-config.js";
import { Context } from "../../runtime/context.js";
import { RuntimeCallerFactory } from "../../runtime/caller-factory.js";
import { callerMetadata } from "../../runtime/caller-metadata.js";
import { stringAttribute } from "../../runtime/environment/tracing/index.js";
import {} from "../../runtime/serde/registry.js";
import { makeJoinStorage } from "../../runtime/store/join-storage.js";
import { RuntimeTaskRegistry } from "../../runtime/task-registry.js";
import { WorkflowPriorityTaskPool, WorkflowTaskPool } from "./workflow-pool.js";
import { WorkflowMetrics, WorkflowTracing, workflowLogger } from "./workflow-telemetry.js";
/**
 * Workflow-isolate implementation of the ordinary graph environment.
 *
 * It deliberately owns no sockets, filesystem access, SDK clients, logging,
 * metrics, or tracing exporters. Existing operators and configured call
 * semantics are reused; Temporal owns the durable execution and timer.
 */
export class TemporalWorkflowEnvironment {
    #config;
    #serviceId;
    #serdeRegistry;
    #streams = new Map();
    #dataSources = new Map();
    #dataSinks = new Map();
    #connectors = new Map();
    #storages = new Set();
    #buildables = new Set();
    #linkCallCounts = new Map();
    #tasks;
    #taskPools;
    #priorityTaskPools;
    #logger;
    #metrics;
    #tracing;
    #callerFactory;
    #failureSignal = Promise.withResolvers();
    #failure;
    #started = false;
    constructor(config, serviceId, serdeRegistry, telemetry = {}) {
        this.#config = new RuntimeConfig(config);
        this.#serviceId = serviceId;
        this.#serdeRegistry = serdeRegistry;
        this.#logger = telemetry.logger ?? workflowLogger;
        this.#metrics = telemetry.metrics ?? new WorkflowMetrics();
        this.#tracing = telemetry.tracing ?? new WorkflowTracing();
        this.#tasks = new RuntimeTaskRegistry((error) => {
            this.recordFailure(error);
        });
        const pools = this.makePools();
        this.#taskPools = pools.task;
        this.#priorityTaskPools = pools.priority;
        this.#callerFactory = new RuntimeCallerFactory({
            config: () => this.runtimeConfig(),
            serviceId,
            taskPools: this.#taskPools,
            priorityTaskPools: this.#priorityTaskPools,
            tasks: this.#tasks,
            onRejected: (error) => {
                this.recordFailure(error);
            }
        });
    }
    runtimeConfig() {
        return this.#config;
    }
    serviceConfig() {
        const config = this.runtimeConfig().serviceById(this.#serviceId);
        if (config === undefined)
            throw new Error(`service config ${String(this.#serviceId)} not found`);
        return config;
    }
    registerStream(stream) {
        if (this.#streams.has(stream.id))
            throw new Error(`duplicate runtime stream id ${String(stream.id)}`);
        this.#streams.set(stream.id, stream);
    }
    registerStorage(storage) {
        if (this.#storages.has(storage))
            throw new Error("storage is already registered");
        this.#storages.add(storage);
    }
    createKeyValueJoinStorage(storageType, config, stream) {
        void stream;
        return makeJoinStorage(storageType, this, config);
    }
    storages() {
        return [...this.#storages];
    }
    addDataSource(dataSource) {
        this.#dataSources.set(dataSource.id, dataSource);
    }
    dataSourceById(id) {
        return this.#dataSources.get(id);
    }
    dataSources() {
        return [...this.#dataSources.values()];
    }
    addDataSink(dataSink) {
        this.#dataSinks.set(dataSink.id, dataSink);
    }
    dataSinkById(id) {
        return this.#dataSinks.get(id);
    }
    dataSinks() {
        return [...this.#dataSinks.values()];
    }
    addManagedDataConnector(connector) {
        const existing = this.#connectors.get(connector.id);
        if (existing !== undefined && existing !== connector) {
            throw new Error(`managed connector ${String(connector.id)} is already registered`);
        }
        this.#connectors.set(connector.id, connector);
    }
    managedDataConnectorById(id) {
        return this.#connectors.get(id);
    }
    managedDataConnectors() {
        return [...this.#connectors.values()];
    }
    log() {
        return this.#logger;
    }
    metrics() {
        return this.#metrics;
    }
    tracing() {
        return this.#tracing;
    }
    registerHttpHandler(path, handler) {
        void path;
        void handler;
        throw new Error("HTTP handlers are unavailable in a Temporal Workflow");
    }
    httpServer() {
        throw new Error("HTTP server is unavailable in a Temporal Workflow");
    }
    registerRuntimeBuildable(buildable) {
        if (this.#buildables.has(buildable))
            throw new Error("runtime buildable is already registered");
        this.#buildables.add(buildable);
    }
    streamById(id) {
        return this.#streams.get(id);
    }
    runtimeStreamIds() {
        return new Set(this.#streams.keys());
    }
    graphLinks() {
        const links = [];
        for (const stream of this.#streams.values()) {
            if (!isTypedStream(stream))
                continue;
            for (const consumer of stream.consumers())
                links.push({ from: stream.id, to: consumer.id });
        }
        return links.sort((left, right) => left.from - right.from || left.to - right.to);
    }
    runtimeStreams() {
        return [...this.#streams.values()];
    }
    linkCallCount(from, to) {
        return this.#linkCallCounts.get(graphLinkKey(from, to)) ?? 0;
    }
    async buildRuntimeStreams() {
        for (const buildable of this.#buildables)
            await buildable.build();
    }
    validateRuntimeTopology() {
        const links = new Set(this.graphLinks().map(({ from, to }) => graphLinkKey(from, to)));
        for (const config of this.runtimeConfig().config().streams) {
            if (config.type === "Error" ||
                (config.idService !== 0 && config.idService !== this.#serviceId))
                continue;
            if (!this.#streams.has(config.id)) {
                throw new Error(`runtime stream ${config.name} (${String(config.id)}) is not registered`);
            }
            for (const source of [config.idSource, ...config.idSources]) {
                if (source !== 0 && !links.has(graphLinkKey(source, config.id))) {
                    throw new Error(`runtime graph link from=${String(source)} to=${String(config.id)} is missing`);
                }
            }
        }
    }
    serdeRegistry() {
        return this.#serdeRegistry;
    }
    serde(type) {
        return this.#serdeRegistry.require(type);
    }
    serdeByName(name) {
        return this.#serdeRegistry.requireByName(name);
    }
    // The generic is required by RuntimeEnvironment's assertion contract.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
    assertSerdeValue(name, value) {
        this.#serdeRegistry.assertByName(name, value);
    }
    streamValueSerde(streamId) {
        return this.#serdeRegistry.requireStreamValue(streamId);
    }
    streamErrorSerde(streamId) {
        return this.#serdeRegistry.requireStreamError(streamId);
    }
    taskPool(name) {
        return this.#taskPools.get(name);
    }
    priorityTaskPool(name) {
        return this.#priorityTaskPools.get(name);
    }
    makeCaller(source, consumer) {
        const caller = this.#callerFactory.create(source, consumer);
        const metadata = callerMetadata(caller);
        const traceAttributes = this.#tracing === undefined
            ? undefined
            : [
                stringAttribute("from", source.name),
                stringAttribute("to", consumer.name),
                ...(metadata === undefined ? [] : [stringAttribute("type", metadata.type)]),
                ...(metadata?.taskPoolName === undefined
                    ? []
                    : [stringAttribute("taskpoolname", metadata.taskPoolName)])
            ];
        return new WorkflowInstrumentedCaller(caller, this.makeLinkRecorder(source, consumer), this.#tracing?.tracer(this.serviceConfig().name), traceAttributes);
    }
    makeLinkRecorder(source, consumer) {
        const key = graphLinkKey(source.id, consumer.id);
        const counter = this.#metrics.enabled()
            ? this.#metrics
                .scope("stream", { service: this.serviceConfig().name })
                .counter("messages_total", "Total number of messages processed by stream link", {
                from: source.name,
                to: consumer.name
            })
            : undefined;
        return (context) => {
            this.#linkCallCounts.set(key, (this.#linkCallCounts.get(key) ?? 0) + 1);
            counter?.inc(context);
        };
    }
    delay(context, delayMs, execute) {
        void context;
        void delayMs;
        void execute;
        throw new Error("Temporal Workflow Delay must use its durable execution context");
    }
    async start() {
        if (this.#started)
            return;
        await this.buildRuntimeStreams();
        this.validateRuntimeTopology();
        const context = Context.background();
        for (const storage of this.#storages)
            await storage.start(context);
        for (const pool of this.#taskPools.values())
            pool.start();
        for (const pool of this.#priorityTaskPools.values())
            pool.start();
        this.#started = true;
    }
    async finish() {
        if (!this.#started)
            return;
        try {
            await this.waitForQuiescence();
        }
        catch {
            // The recorded graph failure is rethrown after deterministic cleanup.
        }
        this.#tasks.stopAdmission();
        const context = Context.background();
        for (const pool of this.#taskPools.values())
            await pool.stop();
        for (const pool of this.#priorityTaskPools.values())
            await pool.stop();
        await this.#tasks.drain();
        for (const storage of this.#storages)
            await storage.stop(context);
        this.#started = false;
        this.throwIfFailed();
    }
    async waitForCompletion(result) {
        if (result === undefined) {
            await this.waitForQuiescence();
            return undefined;
        }
        const completed = await Promise.race([
            result.then((value) => ({ kind: "result", value })),
            this.#failureSignal.promise.then(() => ({ kind: "failure" }))
        ]);
        this.throwIfFailed();
        if (completed.kind !== "result")
            throw new Error("Temporal Workflow graph failed");
        await this.waitForQuiescence();
        return completed.value;
    }
    throwIfFailed() {
        if (this.#failure !== undefined)
            throw this.#failure;
    }
    makePools() {
        const task = new Map();
        const priority = new Map();
        const service = this.serviceConfig().name;
        const use = (semantics) => {
            if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics)
                return;
            const name = "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
            const config = this.runtimeConfig().poolByName(name);
            if (config === undefined)
                throw new Error(`pool config ${name} not found`);
            if ("taskPool" in semantics) {
                if (!task.has(name)) {
                    task.set(name, new WorkflowTaskPool(name, config.executorsCount, this.#metrics, service, (error) => {
                        this.recordFailure(error);
                    }));
                }
            }
            else if (!priority.has(name)) {
                priority.set(name, new WorkflowPriorityTaskPool(name, config.executorsCount, this.#metrics, service, (error) => {
                    this.recordFailure(error);
                }));
            }
        };
        for (const link of this.runtimeConfig().config().links)
            use(link.callSemantics);
        for (const service of this.runtimeConfig().config().services)
            use(service.defaultCallSemantics);
        return { task, priority };
    }
    async waitForQuiescence() {
        for (;;) {
            this.throwIfFailed();
            await Promise.all([
                ...[...this.#taskPools.values()].map(async (pool) => pool.waitIdle()),
                ...[...this.#priorityTaskPools.values()].map(async (pool) => pool.waitIdle()),
                this.#tasks.drain()
            ]);
            await Promise.resolve();
            if (this.#tasks.activeCount() === 0 &&
                ![...this.#taskPools.values()].some((pool) => pool.activeCount() > 0 || pool.queueLength() > 0) &&
                ![...this.#priorityTaskPools.values()].some((pool) => pool.activeCount() > 0 || pool.queueLength() > 0))
                return;
        }
    }
    recordFailure(value) {
        if (this.#failure !== undefined)
            return;
        this.#failure = value instanceof Error ? value : new Error(String(value));
        this.#failureSignal.resolve(true);
    }
}
class WorkflowInstrumentedCaller {
    caller;
    recordCall;
    tracer;
    traceAttributes;
    constructor(caller, recordCall, tracer, traceAttributes) {
        this.caller = caller;
        this.recordCall = recordCall;
        this.tracer = tracer;
        this.traceAttributes = traceAttributes;
    }
    isAsync() {
        return this.caller.isAsync();
    }
    consume(context, value) {
        this.recordCall(context);
        if (this.tracer === undefined || !context.samplingEnabled()) {
            return this.caller.consume(context, value);
        }
        const started = this.tracer.start(context, "stream.call", this.traceAttributes);
        let completion;
        try {
            completion = this.caller.consume(started.context, value);
        }
        catch (error) {
            started.span.end();
            throw error;
        }
        if (completion === undefined) {
            started.span.end();
            return;
        }
        return completion.finally(() => {
            started.span.end();
        });
    }
}
function isTypedStream(stream) {
    return "consumers" in stream && typeof stream.consumers === "function";
}
function graphLinkKey(from, to) {
    return `${String(from)}:${String(to)}`;
}
//# sourceMappingURL=workflow-environment.js.map