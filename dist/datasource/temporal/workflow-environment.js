import { RuntimeConfig } from "../../runtime/config/runtime-config.js";
import { Context } from "../../runtime/context.js";
import { RuntimeCallerFactory } from "../../runtime/caller-factory.js";
import { noopLogger } from "../../runtime/environment/log.js";
import { noopMetrics } from "../../runtime/environment/metrics/noop.js";
import { PriorityTaskPool } from "../../runtime/pool/priority-task-pool.js";
import { TaskPool } from "../../runtime/pool/task-pool.js";
import {} from "../../runtime/serde/registry.js";
import { makeJoinStorage } from "../../runtime/store/join-storage.js";
import { RuntimeTaskRegistry } from "../../runtime/task-registry.js";
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
    #callerFactory;
    #failure;
    #started = false;
    constructor(config, serviceId, serdeRegistry) {
        this.#config = new RuntimeConfig(config);
        this.#serviceId = serviceId;
        this.#serdeRegistry = serdeRegistry;
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
        return noopLogger;
    }
    metrics() {
        return noopMetrics;
    }
    tracing() {
        return undefined;
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
        return {
            isAsync: () => caller.isAsync(),
            consume: (context, value) => {
                const key = graphLinkKey(source.id, consumer.id);
                this.#linkCallCounts.set(key, (this.#linkCallCounts.get(key) ?? 0) + 1);
                return caller.consume(context, value);
            }
        };
    }
    makeLinkRecorder(source, consumer) {
        const key = graphLinkKey(source.id, consumer.id);
        return () => {
            this.#linkCallCounts.set(key, (this.#linkCallCounts.get(key) ?? 0) + 1);
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
            await pool.start(context);
        for (const pool of this.#priorityTaskPools.values())
            await pool.start(context);
        this.#started = true;
    }
    async finish() {
        if (!this.#started)
            return;
        await this.waitForQuiescence();
        this.#tasks.stopAdmission();
        const context = Context.background();
        for (const pool of this.#taskPools.values())
            await pool.stop(context);
        for (const pool of this.#priorityTaskPools.values())
            await pool.stop(context);
        await this.#tasks.drain();
        for (const storage of this.#storages)
            await storage.stop(context);
        this.#started = false;
        this.throwIfFailed();
    }
    throwIfFailed() {
        if (this.#failure !== undefined)
            throw this.#failure;
    }
    makePools() {
        const task = new Map();
        const priority = new Map();
        const use = (semantics) => {
            if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics)
                return;
            const name = "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
            const config = this.runtimeConfig().poolByName(name);
            if (config === undefined)
                throw new Error(`pool config ${name} not found`);
            if ("taskPool" in semantics) {
                if (!task.has(name)) {
                    task.set(name, new TaskPool({
                        name,
                        executorsCount: config.executorsCount,
                        onError: (error) => {
                            this.recordFailure(error);
                        }
                    }));
                }
            }
            else if (!priority.has(name)) {
                priority.set(name, new PriorityTaskPool({
                    name,
                    executorsCount: config.executorsCount,
                    onError: (error) => {
                        this.recordFailure(error);
                    }
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
            const poolWork = [...this.#taskPools.values()].some((pool) => pool.activeCount() > 0 || pool.queueLength() > 0) ||
                [...this.#priorityTaskPools.values()].some((pool) => pool.activeCount() > 0 || pool.queueLength() > 0);
            if (this.#tasks.activeCount() === 0 && !poolWork) {
                await Promise.resolve();
                if (this.#tasks.activeCount() === 0 &&
                    ![...this.#taskPools.values()].some((pool) => pool.activeCount() > 0 || pool.queueLength() > 0) &&
                    ![...this.#priorityTaskPools.values()].some((pool) => pool.activeCount() > 0 || pool.queueLength() > 0))
                    return;
            }
            await Promise.resolve();
        }
    }
    recordFailure(value) {
        if (this.#failure !== undefined)
            return;
        this.#failure = value instanceof Error ? value : new Error(String(value));
    }
}
function isTypedStream(stream) {
    return "consumers" in stream && typeof stream.consumers === "function";
}
function graphLinkKey(from, to) {
    return `${String(from)}:${String(to)}`;
}
//# sourceMappingURL=workflow-environment.js.map