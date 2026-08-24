import { callerMetadata } from "../caller-metadata.js";
import { DelayPool } from "../pool/index.js";
import { ServiceHTTPServer } from "../service-http-server.js";
import { makeDefaultSerdeRegistry } from "../serde/index.js";
import { FunctionCaller } from "../stream.js";
import { noopLogger } from "./log.js";
import { noopMetrics } from "./metrics/index.js";
import { stringAttribute } from "./tracing/index.js";
class DirectCallerFactory {
    create(_source, consumer) {
        return new FunctionCaller(consumer);
    }
}
export class ServiceEnvironment {
    #config;
    #serviceId;
    #callerFactory;
    #streams = new Map();
    #dataSources = new Map();
    #dataSinks = new Map();
    #durableTransports = new Map();
    #storages = new Set();
    #buildables = new Set();
    #logger;
    #metrics;
    #tracing;
    #httpServer;
    #linkCallCounts = new Map();
    #taskPools;
    #priorityTaskPools;
    #joinStorageFactory;
    constructor(config, serviceId, callerFactory = new DirectCallerFactory(), delayPool = new DelayPool(), serdeRegistry = makeDefaultSerdeRegistry(), logger = noopLogger, metrics = noopMetrics, tracing, taskPools = new Map(), priorityTaskPools = new Map(), joinStorageFactory) {
        this.#config = config;
        this.#serviceId = serviceId;
        this.#callerFactory = callerFactory;
        this.#delayPool = delayPool;
        this.#serdeRegistry = serdeRegistry;
        this.#logger = logger;
        this.#metrics = metrics;
        this.#tracing = tracing?.enabled() === true ? tracing : undefined;
        this.#taskPools = taskPools;
        this.#priorityTaskPools = priorityTaskPools;
        this.#joinStorageFactory = joinStorageFactory;
        this.#httpServer = new ServiceHTTPServer(() => this.serviceConfig());
        if (metrics.enabled()) {
            const service = this.serviceConfig();
            const serviceScope = metrics.scope("service", { service: service.name });
            serviceScope
                .gauge("info", "Service information (value is always 1)", {
                environment: service.environment
            })
                .set(1);
            serviceScope.counter("config_reloads_total", "Total number of config reload attempts", {
                event: "success"
            });
            serviceScope.counter("config_reloads_total", "Total number of config reload attempts", {
                event: "error"
            });
        }
    }
    runtimeConfig() {
        return this.#config.current();
    }
    taskPool(name) {
        return this.#taskPools.get(name);
    }
    priorityTaskPool(name) {
        return this.#priorityTaskPools.get(name);
    }
    serviceConfig() {
        const config = this.runtimeConfig().serviceById(this.#serviceId);
        if (config === undefined) {
            throw new Error(`service config ${String(this.#serviceId)} not found`);
        }
        return config;
    }
    registerStream(stream) {
        if (this.#streams.has(stream.id)) {
            throw new Error(`duplicate runtime stream id ${String(stream.id)}`);
        }
        this.#streams.set(stream.id, stream);
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
            if (!isTypedStream(stream)) {
                continue;
            }
            for (const consumer of stream.consumers()) {
                links.push({ from: stream.id, to: consumer.id });
            }
        }
        return links.sort((left, right) => left.from - right.from || left.to - right.to);
    }
    runtimeStreams() {
        return [...this.#streams.values()];
    }
    linkCallCount(from, to) {
        return this.#linkCallCounts.get(graphLinkKey(from, to))?.count ?? 0;
    }
    async buildRuntimeStreams() {
        for (const buildable of this.#buildables) {
            await buildable.build();
        }
    }
    validateRuntimeTopology() {
        const runtime = this.runtimeConfig();
        const links = new Set(this.graphLinks().map(({ from, to }) => graphLinkKey(from, to)));
        for (const config of runtime.config().streams) {
            if (config.type === "Error" ||
                (config.idService !== 0 && config.idService !== this.#serviceId)) {
                continue;
            }
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
    registerStorage(storage) {
        if (this.#storages.has(storage)) {
            throw new Error("storage is already registered");
        }
        this.#storages.add(storage);
    }
    createKeyValueJoinStorage(storageType, config, stream) {
        return this.#joinStorageFactory?.(storageType, config, stream);
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
    addDurableTransport(transport) {
        const existing = this.#durableTransports.get(transport.id);
        if (existing !== undefined && existing !== transport) {
            throw new Error(`durable transport ${String(transport.id)} is already registered`);
        }
        this.#durableTransports.set(transport.id, transport);
    }
    durableTransportById(id) {
        return this.#durableTransports.get(id);
    }
    durableTransports() {
        return [...this.#durableTransports.values()];
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
        this.#httpServer.register(path, handler);
    }
    httpServer() {
        return this.#httpServer;
    }
    storages() {
        return [...this.#storages];
    }
    registerRuntimeBuildable(buildable) {
        if (this.#buildables.has(buildable)) {
            throw new Error("runtime buildable is already registered");
        }
        this.#buildables.add(buildable);
    }
    buildables() {
        return [...this.#buildables];
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
        return new InstrumentedCaller(caller, this.makeLinkRecorder(source, consumer), this.#tracing?.tracer(this.serviceConfig().name), traceAttributes);
    }
    makeLinkRecorder(source, consumer) {
        const key = graphLinkKey(source.id, consumer.id);
        const statistics = { count: 0 };
        this.#linkCallCounts.set(key, statistics);
        const counter = this.#metrics.enabled()
            ? this.#metrics
                .scope("stream", { service: this.serviceConfig().name })
                .counter("messages_total", "Total number of messages processed by stream link", {
                from: source.name,
                to: consumer.name
            })
            : undefined;
        return (context) => {
            statistics.count += 1;
            counter?.inc(context);
        };
    }
    #delayPool;
    #serdeRegistry;
    serdeRegistry() {
        return this.#serdeRegistry;
    }
    serde(type) {
        return this.#serdeRegistry.require(type);
    }
    serdeByName(name) {
        return this.#serdeRegistry.requireByName(name);
    }
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
    assertSerdeValue(name, value) {
        this.#serdeRegistry.assertByName(name, value);
    }
    streamErrorSerde(streamId) {
        return this.#serdeRegistry.requireStreamError(streamId);
    }
    streamValueSerde(streamId) {
        return this.#serdeRegistry.requireStreamValue(streamId);
    }
    delay(context, delayMs, execute) {
        this.#delayPool.delay(context, delayMs, execute);
    }
    delayPool() {
        return this.#delayPool;
    }
}
class InstrumentedCaller {
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
//# sourceMappingURL=runtime-environment.js.map