import { RuntimeDataConnector } from "./data-connector.js";
import { DataConnectorType } from "./config/types.js";
import {} from "./environment/metrics/metrics.js";
import { err, str } from "./environment/log.js";
/** Common typed collector context passed to datasink endpoint handlers. */
// T is intentionally retained in the public signature to match the canonical
// SinkStreamContext<T, R, E>; the context only emits R and E by design.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export class SinkStreamContext {
    stream;
    logger;
    #collector;
    #errorCollector;
    constructor(stream, logger, collector, errorCollector) {
        this.stream = stream;
        this.logger = logger;
        this.#collector = collector;
        this.#errorCollector = errorCollector;
    }
    collect(context, value) {
        return this.#collector.out(context, value);
    }
    errorCollect(context, value) {
        return this.#errorCollector.out(context, value);
    }
}
export function makeSinkStreamContext(stream, collector, errorCollector) {
    return new SinkStreamContext(stream, stream.runtimeEnvironment().log(), collector, errorCollector);
}
export class OutputDataSink extends RuntimeDataConnector {
    #endpoints = new Map();
    addEndpoint(endpoint) {
        this.#endpoints.set(endpoint.id, endpoint);
    }
    endpoint(id) {
        return this.#endpoints.get(id);
    }
    endpoints() {
        return [...this.#endpoints.values()];
    }
}
export class DataSinkEndpoint {
    #consumers = [];
    #dataSink;
    id;
    name;
    #metrics;
    constructor(dataSink, endpointId) {
        const config = dataSink.runtimeEnvironment().runtimeConfig().endpointById(endpointId);
        if (config === undefined) {
            throw new Error(`endpoint config ${String(endpointId)} not found`);
        }
        if (config.idDataConnector !== dataSink.id) {
            throw new Error(`endpoint ${config.name} belongs to connector ${String(config.idDataConnector)}, not ${String(dataSink.id)}`);
        }
        this.#dataSink = dataSink;
        this.id = endpointId;
        this.name = config.name;
        this.#metrics = makeDataSinkEndpointMetrics(dataSink, this.name);
    }
    config() {
        const config = this.runtimeEnvironment().runtimeConfig().endpointById(this.id);
        if (config === undefined) {
            throw new Error(`endpoint config ${String(this.id)} not found`);
        }
        return config;
    }
    runtimeEnvironment() {
        return this.#dataSink.runtimeEnvironment();
    }
    dataSink() {
        return this.#dataSink;
    }
    dataConnector() {
        return this.#dataSink;
    }
    addEndpointConsumer(consumer) {
        this.#consumers.push(consumer);
    }
    endpointConsumers() {
        return [...this.#consumers];
    }
    onBeginRequestFailed(context, error) {
        this.runtimeEnvironment()
            .log()
            .error(context, "BeginRequest failed", str("endpoint", this.name), err(error));
        this.#metrics?.beginRequestFailed.inc(context);
    }
    onLateResult(context, streamId) {
        this.runtimeEnvironment()
            .log()
            .warn(context, "late result for sink endpoint", str("endpoint", this.name), str("stream_id", streamId));
        this.#metrics?.lateResult.inc(context);
    }
    onRequestStart(context) {
        void context;
        if (this.#metrics === undefined) {
            return undefined;
        }
        this.#metrics.activeRequests.inc();
        return Date.now();
    }
    onRequestEnd(context, started, error) {
        if (this.#metrics === undefined || started === undefined) {
            return;
        }
        this.#metrics.activeRequests.dec();
        this.#metrics.requestDuration.observe(context, (Date.now() - started) / 1_000);
        if (error === undefined) {
            this.#metrics.messagesTotal.inc(context);
        }
        else {
            this.#metrics.requestErrors.inc(context);
        }
    }
}
function makeDataSinkEndpointMetrics(dataSink, endpointName) {
    const metrics = dataSink.runtimeEnvironment().metrics();
    if (!metrics.enabled()) {
        return undefined;
    }
    const scope = metrics.scope("datasink_endpoint", {
        connector: dataSink.name,
        endpoint: endpointName,
        protocol: dataSink.config().type === DataConnectorType.GRPC ? "grpc" : ""
    });
    const events = scope.counterVec("events_total", "Total number of events in data sink endpoint");
    return {
        beginRequestFailed: events.with({ event: "begin_request_failed" }),
        lateResult: events.with({ event: "late_result" }),
        requestErrors: events.with({ event: "request_error" }),
        messagesTotal: scope.counter("messages_total", "Total number of successfully processed messages in data sink endpoint"),
        requestDuration: scope.histogram("request_duration_seconds", "Request duration in seconds for data sink endpoint"),
        activeRequests: scope.gauge("active_requests", "Number of active requests in data sink endpoint")
    };
}
export class DataSinkEndpointConsumer {
    #endpoint;
    #stream;
    constructor(endpoint, stream) {
        this.#endpoint = endpoint;
        this.#stream = stream;
    }
    endpoint() {
        return this.#endpoint;
    }
    stream() {
        return this.#stream;
    }
}
export class DataSinkEndpointConsumerWithResult {
    #endpoint;
    #stream;
    constructor(endpoint, stream) {
        this.#endpoint = endpoint;
        this.#stream = stream;
    }
    endpoint() {
        return this.#endpoint;
    }
    stream() {
        return this.#stream;
    }
}
//# sourceMappingURL=data-sink.js.map