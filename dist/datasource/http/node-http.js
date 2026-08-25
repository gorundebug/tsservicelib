import { createServer } from "node:http";
import { DataSourceEndpoint, DataSourceEndpointConsumer, FunctionCollector, InputDataSource, MessageContext, RotatingMap, RuntimeTaskRegistry, STREAM_ID_HEADER, TRACE_SAMPLING_HEADER, applyDataSourceEndpointTracing, errorFromUnknown, boolAttribute, makeStreamContext, newStreamId, requireHttpDataConnectorConfig, requireHttpEndpointConfig, spanError, stringAttribute } from "../../runtime/index.js";
const PENDING_ROTATION_INTERVAL_MS = 30_000;
class HttpResult {
    handlerState;
    data;
    span;
    #callbacks = new Map();
    #done;
    #resolveDone;
    #doneCalled = false;
    #retiring = false;
    #activeCallbacks = 0;
    #retired;
    #resolveRetired;
    constructor(handlerState, data, span) {
        this.handlerState = handlerState;
        this.data = data;
        this.span = span;
        this.#done = new Promise((resolve) => {
            this.#resolveDone = resolve;
        });
    }
    setResultCallback(messageId, callback) {
        this.#callbacks.set(messageId, callback);
    }
    done() {
        if (this.#doneCalled) {
            return;
        }
        this.#doneCalled = true;
        this.span?.addEvent("done_called");
        this.#resolveDone?.();
        this.#resolveDone = undefined;
    }
    wait() {
        return this.#done;
    }
    callback(messageId) {
        return this.#callbacks.get(messageId);
    }
    removeCallback(messageId, callback) {
        if (this.#callbacks.get(messageId) !== callback) {
            return false;
        }
        return this.#callbacks.delete(messageId);
    }
    beginCallback() {
        if (this.#retiring) {
            return false;
        }
        this.#activeCallbacks += 1;
        return true;
    }
    endCallback() {
        this.#activeCallbacks -= 1;
        if (this.#retiring && this.#activeCallbacks === 0) {
            this.#resolveRetired?.();
            this.#resolveRetired = undefined;
        }
    }
    async retire() {
        this.#retiring = true;
        if (this.#activeCallbacks !== 0) {
            this.#retired ??= new Promise((resolve) => {
                this.#resolveRetired = resolve;
            });
            await this.#retired;
        }
        return this.#doneCalled;
    }
}
class NodeHttpInputEndpoint extends DataSourceEndpoint {
    method;
    path;
    #consumer;
    #requestHandler;
    constructor(dataSource, config) {
        super(dataSource, config.id);
        if (config.httpMethodType !== "GET" && config.httpMethodType !== "POST") {
            throw new Error(`no method specified for HTTP endpoint ${config.name}`);
        }
        if (config.path.length === 0) {
            throw new Error(`no path specified for HTTP endpoint ${config.name}`);
        }
        this.method = config.httpMethodType;
        this.path = config.path;
        this.#requestHandler = (request, response) => {
            void this.serve(request, response).catch((error) => {
                if (!response.headersSent) {
                    response.statusCode = 500;
                    response.end("internal server error");
                }
                else if (!response.writableEnded) {
                    response.destroy(errorFromUnknown(error));
                }
            });
        };
    }
    bindConsumer(consumer) {
        if (this.#consumer !== undefined) {
            throw new Error(`consumer already assigned to HTTP endpoint ${this.name}`);
        }
        this.#consumer = consumer;
        this.addEndpointConsumer(consumer);
    }
    start(context) {
        return this.#consumer?.start(context) ?? Promise.resolve();
    }
    stop(context) {
        return this.#consumer?.stop(context) ?? Promise.resolve();
    }
    handler() {
        return this.#requestHandler;
    }
    async serve(request, response) {
        if (request.method !== this.method) {
            this.onInvalidHttpMethod(contextFromRequest(request), request.method ?? "");
            response.setHeader("allow", this.method);
            response.statusCode = 405;
            response.end();
            return;
        }
        if (this.#consumer === undefined) {
            response.statusCode = 503;
            response.end("endpoint consumer is not registered");
            return;
        }
        await this.#consumer.serveHttp(request, response);
    }
}
export class NodeHttpDataSource extends InputDataSource {
    #routes = new Map();
    #server;
    #started = false;
    constructor(connectorId, environment) {
        super(connectorId, environment);
        requireHttpDataConnectorConfig(this.config());
    }
    addHttpEndpoint(endpoint) {
        if (this.#routes.has(endpoint.path)) {
            throw new Error(`HTTP path ${endpoint.path} is already registered`);
        }
        this.#routes.set(endpoint.path, endpoint);
        this.addEndpoint(endpoint);
        const config = requireHttpDataConnectorConfig(this.config());
        if (!config.useDedicatedListener) {
            this.runtimeEnvironment().registerHttpHandler(endpoint.path, endpoint.handler());
        }
    }
    async start(context) {
        if (this.#started) {
            throw new Error(`HTTP data source ${this.name} is already started`);
        }
        this.#started = true;
        try {
            for (const endpoint of this.httpEndpoints()) {
                await endpoint.start(context);
            }
            const config = requireHttpDataConnectorConfig(this.config());
            if (!config.useDedicatedListener) {
                return;
            }
            if (config.host === undefined ||
                config.host.length === 0 ||
                config.port === undefined ||
                config.port === 0) {
                throw new Error(`host and port are required for HTTP data connector ${this.name}`);
            }
            const server = createServer((request, response) => {
                this.route(request, response);
            });
            this.#server = server;
            await listen(server, config.port, config.host, context.signal());
        }
        catch (error) {
            this.#started = false;
            const server = this.#server;
            this.#server = undefined;
            if (server?.listening === true) {
                try {
                    await closeServer(server, context.signal());
                }
                catch {
                    // Preserve the original startup failure after best-effort rollback.
                }
            }
            try {
                await this.stopEndpoints(context);
            }
            catch {
                // Preserve the original startup failure after best-effort rollback.
            }
            throw error;
        }
    }
    async stop(context) {
        if (!this.#started) {
            return;
        }
        this.#started = false;
        const server = this.#server;
        this.#server = undefined;
        try {
            await this.stopEndpoints(context);
        }
        finally {
            if (server !== undefined) {
                await closeServer(server, context.signal());
            }
        }
    }
    httpEndpoints() {
        return [...this.#routes.values()];
    }
    async stopEndpoints(context) {
        for (const endpoint of this.httpEndpoints()) {
            await endpoint.stop(context);
        }
    }
    route(request, response) {
        let path;
        try {
            path = new URL(request.url ?? "", "http://service.local").pathname;
        }
        catch {
            response.statusCode = 400;
            response.end("invalid request target");
            return;
        }
        const endpoint = this.#routes.get(path);
        if (endpoint === undefined) {
            response.statusCode = 404;
            response.end();
            return;
        }
        endpoint.handler()(request, response);
    }
}
class NodeHttpEndpointConsumer {
    #base;
    #handler;
    #streamContext;
    #hasResult;
    #tracer;
    #tasks = new RuntimeTaskRegistry();
    #pending;
    #started = false;
    #stopped = false;
    constructor(endpoint, stream, handler) {
        this.#base = new DataSourceEndpointConsumer(endpoint, stream);
        this.#handler = handler;
        this.#hasResult = stream.resultStream() !== undefined;
        this.#streamContext = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((context, value) => this.consume(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
        if (this.#hasResult) {
            stream.setResultConsumer({
                consume: (context, value) => this.consumeResult(context, value)
            });
        }
    }
    endpoint() {
        return this.#base.endpoint();
    }
    stream() {
        return this.#base.stream();
    }
    consume(context, value) {
        return this.#base.consume(context, value);
    }
    start(context) {
        if (this.#started) {
            return Promise.reject(new Error(`HTTP endpoint ${this.endpoint().name} is already started`));
        }
        if (this.#stopped) {
            return Promise.reject(new Error(`HTTP endpoint ${this.endpoint().name} is stopped`));
        }
        this.#started = true;
        if (!this.#hasResult) {
            return Promise.resolve();
        }
        if (this.#pending !== undefined) {
            return Promise.reject(new Error(`HTTP endpoint ${this.endpoint().name} is already started`));
        }
        this.#pending = new RotatingMap(PENDING_ROTATION_INTERVAL_MS);
        this.#pending.start(context);
        return Promise.resolve();
    }
    stop(context) {
        if (!this.#started) {
            return Promise.resolve();
        }
        this.#started = false;
        this.#stopped = true;
        this.#tasks.stopAdmission();
        return drainAcceptedTasks(this.#tasks, context).finally(() => {
            this.#pending?.stop(context);
        });
    }
    serveHttp(request, response) {
        if (!this.#started) {
            response.statusCode = 503;
            response.end("HTTP endpoint is not accepting requests");
            return Promise.resolve();
        }
        const cancellation = requestCancellation(request, response);
        return this.#tasks.admit((lifecycleSignal) => this.serveAccepted(request, response, cancellation, lifecycleSignal), cancellation.signal);
    }
    async serveAccepted(request, response, cancellation, lifecycleSignal) {
        let context = applyDataSourceEndpointTracing(contextFromRequest(request, lifecycleSignal), this.stream().runtimeEnvironment(), this.endpoint().id);
        const data = { request, response };
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "http.input", [
                stringAttribute("stream", this.stream().name),
                stringAttribute("endpoint", this.endpoint().name),
                stringAttribute("method", request.method ?? ""),
                stringAttribute("path", requestPath(request))
            ]);
            context = started.context;
            span = started.span;
        }
        let state;
        try {
            const started = await this.#handler.beginRequest(context, this.#streamContext, data);
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            this.endpoint().onBeginRequestFailed(context, failure);
            try {
                cancellation.complete();
            }
            finally {
                span?.end();
            }
            return;
        }
        span?.addEvent("begin_request");
        const requestStarted = this.endpoint().onRequestStart(context);
        let streamId = context.streamId();
        if (streamId === undefined) {
            streamId = newStreamId();
            context = context.withStreamId(streamId);
        }
        span?.setAttributes([
            stringAttribute("stream_id", streamId),
            boolAttribute("has_result", this.#hasResult)
        ]);
        const result = new HttpResult(state, data, span);
        let pendingAdded = false;
        let requestError;
        let resultWaitFailed = false;
        try {
            if (this.#hasResult) {
                this.pending().set(streamId, result);
                this.endpoint().onPendingAdd(context, streamId);
                pendingAdded = true;
            }
            try {
                await this.#handler.consumeMessage(context, this.#streamContext, state, data, result);
            }
            catch (error) {
                const failure = errorFromUnknown(error);
                span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
                throw failure;
            }
            span?.addEvent("consume_message");
            if (this.#hasResult) {
                const waitFailure = await waitForDoneOrCancellation(result, context.signal());
                if (waitFailure !== undefined) {
                    resultWaitFailed = true;
                    throw waitFailure;
                }
                span?.addEvent("done_received");
            }
        }
        catch (error) {
            requestError = errorFromUnknown(error);
        }
        finally {
            if (pendingAdded) {
                const resultCompleted = await result.retire();
                if (resultWaitFailed && resultCompleted)
                    requestError = undefined;
                this.pending().pop(streamId);
                this.endpoint().onPendingRemove(context, streamId);
            }
            if (requestError !== undefined) {
                spanError(span, requestError);
                if (context.cancelled()) {
                    span?.addEvent("context_cancelled", [stringAttribute("error", requestError.message)]);
                }
            }
            try {
                await this.#handler.endRequest(context, this.#streamContext, requestError, state, data);
            }
            finally {
                try {
                    this.endpoint().onRequestEnd(context, requestStarted, requestError);
                }
                finally {
                    try {
                        cancellation.complete();
                    }
                    finally {
                        span?.end();
                    }
                }
            }
        }
    }
    async consumeResult(context, value) {
        const streamId = context.streamId();
        if (streamId === undefined) {
            this.endpoint().onMissingStreamId(context);
            return;
        }
        if (this.#pending === undefined) {
            this.endpoint().onLateResult(context, streamId);
            return;
        }
        const [result, found] = this.#pending.get(streamId);
        if (!found || result?.beginCallback() !== true) {
            this.endpoint().onLateResult(context, streamId);
            return;
        }
        try {
            const messageId = this.#handler.getMessageId(context, this.#streamContext, result.handlerState, value);
            const callback = result.callback(messageId);
            if (callback === undefined) {
                this.endpoint().onUnknownMessageId(context, streamId, messageId);
                result.span?.addEvent("unknown_message_id", [stringAttribute("message_id", messageId)]);
                return;
            }
            if (await callback(context, this.#streamContext, result.handlerState, value, result.data)) {
                if (!result.removeCallback(messageId, callback)) {
                    this.endpoint().onDuplicateMessageId(context, streamId, messageId);
                    result.span?.addEvent("duplicate_message_id", [stringAttribute("message_id", messageId)]);
                }
            }
            result.span?.addEvent("result_consumed", [stringAttribute("message_id", messageId)]);
        }
        finally {
            result.endCallback();
        }
    }
    pending() {
        if (this.#pending === undefined) {
            throw new Error(`HTTP endpoint ${this.endpoint().name} is not started`);
        }
        return this.#pending;
    }
}
async function drainAcceptedTasks(tasks, context) {
    try {
        await tasks.drain(context.remainingMs());
    }
    catch (error) {
        tasks.cancel(context.signal().reason ?? error);
        await tasks.drain();
    }
}
export function makeNodeHttpEndpointConsumer(stream, handler) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = requireHttpEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
    if (dataSource.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new NodeHttpInputEndpoint(dataSource, endpointConfig);
    const consumer = new NodeHttpEndpointConsumer(endpoint, stream, handler);
    endpoint.bindConsumer(consumer);
    dataSource.addHttpEndpoint(endpoint);
    return [consumer, endpoint.handler()];
}
function getOrCreateDataSource(connectorId, environment) {
    const existing = environment.dataSourceById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof NodeHttpDataSource)) {
            throw new Error(`data source ${String(connectorId)} is not a Node HTTP data source`);
        }
        return existing;
    }
    const dataSource = new NodeHttpDataSource(connectorId, environment);
    environment.addDataSource(dataSource);
    return dataSource;
}
function contextFromRequest(request, signal) {
    const metadata = new Map();
    for (const name of [
        STREAM_ID_HEADER,
        TRACE_SAMPLING_HEADER,
        "traceparent",
        "tracestate",
        "baggage"
    ]) {
        const value = request.headers[name];
        const first = Array.isArray(value) ? value[0] : value;
        if (first !== undefined) {
            metadata.set(name, first);
        }
    }
    return new MessageContext(signal).withMetadata(metadata);
}
function requestPath(request) {
    try {
        return new URL(request.url ?? "", "http://service.local").pathname;
    }
    catch {
        return "";
    }
}
function requestCancellation(request, response) {
    const controller = new AbortController();
    const abort = () => {
        if (!response.writableEnded) {
            controller.abort(new Error("HTTP peer disconnected"));
        }
    };
    request.once("aborted", abort);
    response.once("close", abort);
    if (request.destroyed && !request.complete) {
        abort();
    }
    return {
        signal: controller.signal,
        complete() {
            // Go's net/http cancels Request.Context when ServeHTTP returns. Preserve
            // that lifecycle boundary so detached graph branches (for example the
            // soft-deadline branch) are cancelled as soon as the response is done.
            if (!controller.signal.aborted) {
                controller.abort(new Error("HTTP request completed"));
            }
            request.removeListener("aborted", abort);
            response.removeListener("close", abort);
        }
    };
}
function waitForDoneOrCancellation(result, signal) {
    if (signal.aborted) {
        return Promise.resolve(abortReason(signal, "HTTP request cancelled"));
    }
    return new Promise((resolve) => {
        const cancelled = () => {
            resolve(abortReason(signal, "HTTP request cancelled"));
        };
        signal.addEventListener("abort", cancelled, { once: true });
        void result.wait().then(() => {
            signal.removeEventListener("abort", cancelled);
            resolve(undefined);
        }, (error) => {
            signal.removeEventListener("abort", cancelled);
            resolve(errorFromUnknown(error));
        });
    });
}
function listen(server, port, host, signal) {
    if (signal.aborted) {
        return Promise.reject(abortReason(signal, "HTTP startup cancelled"));
    }
    return new Promise((resolve, reject) => {
        const listening = () => {
            cleanup();
            resolve();
        };
        const failed = (error) => {
            cleanup();
            reject(error);
        };
        const cancelled = () => {
            cleanup();
            server.close();
            reject(abortReason(signal, "HTTP startup cancelled"));
        };
        const cleanup = () => {
            server.removeListener("listening", listening);
            server.removeListener("error", failed);
            signal.removeEventListener("abort", cancelled);
        };
        server.once("listening", listening);
        server.once("error", failed);
        signal.addEventListener("abort", cancelled, { once: true });
        server.listen(port, host);
    });
}
function closeServer(server, signal) {
    return new Promise((resolve, reject) => {
        const cancelled = () => {
            server.closeAllConnections();
        };
        signal.addEventListener("abort", cancelled, { once: true });
        if (signal.aborted) {
            cancelled();
        }
        server.close((error) => {
            signal.removeEventListener("abort", cancelled);
            if (error === undefined) {
                resolve();
            }
            else {
                reject(error);
            }
        });
    });
}
function abortReason(signal, fallback) {
    return signal.reason === undefined ? new Error(fallback) : errorFromUnknown(signal.reason);
}
//# sourceMappingURL=node-http.js.map