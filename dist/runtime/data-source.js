import { performance } from "node:perf_hooks";
import { DataConnectorType } from "./config/index.js";
import { RuntimeDataConnector } from "./data-connector.js";
import { err, str } from "./environment/index.js";
/** Apply the current reloadable source-endpoint tracing policy to one event. */
export function applyDataSourceEndpointTracing(context, environment, endpointId) {
    return environment.runtimeConfig().endpointById(endpointId)?.tracingEnabled === true
        ? context.withSampling(true)
        : context;
}
/** Common typed collector context passed to datasource endpoint handlers. */
export class StreamContext {
    stream;
    resultStream;
    logger;
    #collector;
    #errorCollector;
    constructor(stream, resultStream, logger, collector, errorCollector) {
        this.stream = stream;
        this.resultStream = resultStream;
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
export function makeStreamContext(stream, resultStream, collector, errorCollector) {
    return new StreamContext(stream, resultStream, stream.runtimeEnvironment().log(), collector, errorCollector);
}
export class InputDataSource extends RuntimeDataConnector {
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
export class DataSourceEndpoint {
    #consumers = [];
    #dataSource;
    id;
    name;
    #metrics;
    #pendingStarted = new Map();
    constructor(dataSource, endpointId) {
        const config = dataSource.runtimeEnvironment().runtimeConfig().endpointById(endpointId);
        if (config === undefined) {
            throw new Error(`endpoint config ${String(endpointId)} not found`);
        }
        if (config.idDataConnector !== dataSource.id) {
            throw new Error(`endpoint ${config.name} belongs to connector ${String(config.idDataConnector)}, not ${String(dataSource.id)}`);
        }
        this.#dataSource = dataSource;
        this.id = endpointId;
        this.name = config.name;
        this.#metrics = makeDataSourceEndpointMetrics(dataSource, this.name, () => this.oldestPendingAge());
    }
    config() {
        const config = this.runtimeEnvironment().runtimeConfig().endpointById(this.id);
        if (config === undefined) {
            throw new Error(`endpoint config ${String(this.id)} not found`);
        }
        return config;
    }
    runtimeEnvironment() {
        return this.#dataSource.runtimeEnvironment();
    }
    dataSource() {
        return this.#dataSource;
    }
    dataConnector() {
        return this.#dataSource;
    }
    addEndpointConsumer(consumer) {
        this.#consumers.push(consumer);
    }
    endpointConsumers() {
        return [...this.#consumers];
    }
    onMissingStreamId(context) {
        this.runtimeEnvironment()
            .log()
            .error(context, "consumeResult called without streamID", str("endpoint", this.name));
        this.#metrics?.missingStreamId.inc(context);
    }
    onLateResult(context, sessionId) {
        this.runtimeEnvironment()
            .log()
            .warn(context, "consumeResult: session not found in pending", str("endpoint", this.name), str("session_id", sessionId));
        this.#metrics?.lateResult.inc(context);
    }
    onUnknownMessageId(context, sessionId, messageId) {
        this.runtimeEnvironment()
            .log()
            .warn(context, "consumeResult: unknown message ID", str("endpoint", this.name), str("message_id", messageId), str("session_id", sessionId));
        this.#metrics?.unknownMessageId.inc(context);
    }
    onDuplicateMessageId(context, sessionId, messageId) {
        this.runtimeEnvironment()
            .log()
            .warn(context, "consumeResult: duplicate message ID", str("endpoint", this.name), str("message_id", messageId), str("session_id", sessionId));
        this.#metrics?.duplicateMessageId.inc(context);
    }
    onPendingAdd(context, streamId) {
        void context;
        if (this.#metrics === undefined) {
            return;
        }
        this.#metrics.pendingRequests.inc();
        this.#pendingStarted.set(streamId, performance.now());
    }
    onPendingRemove(context, streamId) {
        void context;
        if (this.#metrics === undefined) {
            return;
        }
        this.#metrics.pendingRequests.dec();
        this.#pendingStarted.delete(streamId);
    }
    onInvalidHttpMethod(context, method) {
        this.runtimeEnvironment()
            .log()
            .warn(context, "invalid HTTP method", str("method", method), str("endpoint", this.name));
        this.#metrics?.invalidHttpMethod.inc(context);
    }
    onBeginRequestFailed(context, error) {
        this.runtimeEnvironment()
            .log()
            .error(context, "BeginRequest failed", str("endpoint", this.name), err(error));
        this.#metrics?.beginRequestFailed.inc(context);
    }
    onRequestStart(context) {
        void context;
        if (this.#metrics === undefined) {
            return undefined;
        }
        this.#metrics.activeRequests.inc();
        return performance.now();
    }
    onRequestEnd(context, started, error) {
        if (this.#metrics === undefined || started === undefined) {
            return;
        }
        this.#metrics.activeRequests.dec();
        this.#metrics.requestDuration.observe(context, (performance.now() - started) / 1_000);
        if (error === undefined) {
            this.#metrics.messagesTotal.inc(context);
        }
        else {
            this.#metrics.requestErrors.inc(context);
        }
    }
    oldestPendingAge() {
        let oldest = Infinity;
        for (const started of this.#pendingStarted.values()) {
            oldest = Math.min(oldest, started);
        }
        return oldest === Infinity ? 0 : (performance.now() - oldest) / 1_000;
    }
}
function makeDataSourceEndpointMetrics(dataSource, endpointName, oldestPendingAge) {
    const metrics = dataSource.runtimeEnvironment().metrics();
    if (!metrics.enabled()) {
        return undefined;
    }
    const scope = metrics.scope("datasource_endpoint", {
        connector: dataSource.name,
        endpoint: endpointName,
        protocol: dataSource.config().type === DataConnectorType.GRPC ? "grpc" : ""
    });
    const events = scope.counterVec("events_total", "Total number of events in data source endpoint");
    scope.observableFloat64Gauge("pending_oldest_age_seconds", "Age in seconds of the oldest pending request awaiting a pipeline result", oldestPendingAge);
    return {
        missingStreamId: events.with({ event: "missing_stream_id" }),
        lateResult: events.with({ event: "late_result" }),
        unknownMessageId: events.with({ event: "unknown_message_id" }),
        duplicateMessageId: events.with({ event: "duplicate_message_id" }),
        invalidHttpMethod: events.with({ event: "invalid_http_method" }),
        beginRequestFailed: events.with({ event: "begin_request_failed" }),
        requestErrors: events.with({ event: "request_error" }),
        messagesTotal: scope.counter("messages_total", "Total number of successfully processed messages in data source endpoint"),
        requestDuration: scope.histogram("request_duration_seconds", "Request duration in seconds for data source endpoint"),
        activeRequests: scope.gauge("active_requests", "Number of active requests in data source endpoint"),
        pendingRequests: scope.gauge("pending_requests", "Number of requests awaiting a pipeline result")
    };
}
export class DataSourceEndpointConsumer {
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
    consume(context, value) {
        return this.#stream.consume(context, value);
    }
}
//# sourceMappingURL=data-source.js.map