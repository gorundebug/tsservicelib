import { Agent as HttpAgent, request as httpRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { Readable } from "node:stream";
import { DataSinkEndpoint, DataSinkEndpointConsumerWithResult, FunctionCollector, OutputDataSink, RuntimeTaskRegistry, SinkStreamContext, int64Attribute, errorFromUnknown, newStreamId, requireHttpDataConnectorConfig, requireHttpEndpointConfig, spanError, stringAttribute } from "../../runtime/index.js";
export class Request {
    context;
    method;
    url;
    headers = new Headers();
    body;
    constructor(context, method, url, body) {
        this.context = context;
        this.method = method;
        this.url = typeof url === "string" ? new URL(url) : url;
        this.body = body;
    }
}
export class Requester {
    #request;
    newRequest(context, method, url, body) {
        const request = new Request(context, method, url, body);
        this.#request = request;
        return request;
    }
    request() {
        return this.#request;
    }
}
export class Response {
    statusCode;
    status;
    headers;
    body;
    constructor(body) {
        this.body = body;
        this.statusCode = body.statusCode ?? 0;
        this.status = `${String(this.statusCode)}${body.statusMessage === undefined ? "" : ` ${body.statusMessage}`}`;
        this.headers = body.headers;
    }
    async read(maxBytes = Number.MAX_SAFE_INTEGER) {
        if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
            throw new RangeError("HTTP response body limit must be a non-negative safe integer");
        }
        const chunks = [];
        let size = 0;
        for await (const chunk of this.body) {
            const value = chunk;
            const bytes = typeof value === "string"
                ? Buffer.from(value)
                : value instanceof Uint8Array
                    ? value
                    : undefined;
            if (bytes === undefined) {
                throw new TypeError("HTTP response emitted a non-byte chunk");
            }
            size += bytes.byteLength;
            if (size > maxBytes) {
                throw new ResponseBodyTooLargeError(maxBytes);
            }
            chunks.push(bytes);
        }
        return Buffer.concat(chunks, size);
    }
    async text(maxBytes) {
        return Buffer.from(await this.read(maxBytes)).toString("utf8");
    }
    async close() {
        if (this.body.complete || this.body.destroyed) {
            return;
        }
        await new Promise((resolve, reject) => {
            const completed = () => {
                cleanup();
                resolve();
            };
            const failed = (error) => {
                cleanup();
                reject(error);
            };
            const cleanup = () => {
                this.body.removeListener("end", completed);
                this.body.removeListener("close", completed);
                this.body.removeListener("error", failed);
            };
            this.body.once("end", completed);
            this.body.once("close", completed);
            this.body.once("error", failed);
            this.body.resume();
        });
    }
}
export class ResponseBodyTooLargeError extends Error {
    limit;
    constructor(limit) {
        super(`HTTP response body exceeds ${String(limit)} bytes`);
        this.name = "ResponseBodyTooLargeError";
        this.limit = limit;
    }
}
export class NodeHttpClient {
    #httpAgent;
    #httpsAgent;
    #closed = false;
    constructor(options = {}) {
        const agentOptions = {
            keepAlive: true,
            maxSockets: options.maxSockets ?? Infinity,
            maxFreeSockets: options.maxFreeSockets ?? 256
        };
        this.#httpAgent = new HttpAgent(agentOptions);
        this.#httpsAgent = new HttpsAgent(agentOptions);
    }
    do(request) {
        if (this.#closed) {
            return Promise.reject(new Error("HTTP client is closed"));
        }
        if (request.context.cancelled()) {
            return Promise.reject(contextError(request.context, "HTTP request context is cancelled"));
        }
        const protocol = request.url.protocol;
        if (protocol !== "http:" && protocol !== "https:") {
            return Promise.reject(new Error(`unsupported HTTP protocol ${protocol}`));
        }
        const transport = protocol === "https:" ? httpsRequest : httpRequest;
        const agent = protocol === "https:" ? this.#httpsAgent : this.#httpAgent;
        return new Promise((resolve, reject) => {
            const outgoing = transport(request.url, {
                method: request.method,
                headers: Object.fromEntries(request.headers.entries()),
                agent,
                signal: request.context.signal()
            }, (incoming) => {
                resolve(new Response(incoming));
            });
            outgoing.once("error", (error) => {
                reject(error);
            });
            const remaining = request.context.remainingMs();
            if (remaining !== undefined) {
                outgoing.setTimeout(Math.max(1, Math.ceil(remaining)), () => {
                    outgoing.destroy(new Error("HTTP request deadline exceeded"));
                });
            }
            const body = request.body;
            if (body instanceof Readable) {
                body.once("error", (error) => {
                    outgoing.destroy(error);
                });
                body.pipe(outgoing);
            }
            else {
                outgoing.end(body);
            }
        });
    }
    close(context) {
        void context;
        if (!this.#closed) {
            this.#closed = true;
            this.#httpAgent.destroy();
            this.#httpsAgent.destroy();
        }
        return Promise.resolve();
    }
}
export class StreamContext extends SinkStreamContext {
    #environment;
    #endpointId;
    #connectorId;
    constructor(stream) {
        const environment = stream.runtimeEnvironment();
        const endpoint = requireHttpEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
        super(stream, environment.log(), new FunctionCollector((context, value) => stream.consumeResult(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        this.#environment = environment;
        this.#endpointId = endpoint.id;
        this.#connectorId = endpoint.idDataConnector;
    }
    get endpointConfig() {
        return requireHttpEndpointConfig(this.#environment.runtimeConfig().endpointById(this.#endpointId));
    }
    get dataConnectorConfig() {
        return requireHttpDataConnectorConfig(this.#environment.runtimeConfig().dataConnectorById(this.#connectorId));
    }
}
class NodeHttpSinkEndpoint extends DataSinkEndpoint {
    #consumer;
    bindConsumer(consumer) {
        if (this.#consumer !== undefined) {
            throw new Error(`consumer already assigned to HTTP sink endpoint ${this.name}`);
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
}
export class NodeHttpDataSink extends OutputDataSink {
    #client;
    #started = false;
    constructor(connectorId, environment, client) {
        super(connectorId, environment);
        requireHttpDataConnectorConfig(this.config());
        this.#client = client;
    }
    client() {
        return this.#client;
    }
    async start(context) {
        if (this.#started) {
            throw new Error(`HTTP data sink ${this.name} is already started`);
        }
        this.#started = true;
        try {
            for (const endpoint of this.httpEndpoints()) {
                await endpoint.start(context);
            }
        }
        catch (error) {
            this.#started = false;
            await this.stopEndpoints(context);
            throw error;
        }
    }
    async stop(context) {
        if (!this.#started) {
            return;
        }
        this.#started = false;
        try {
            await this.stopEndpoints(context);
        }
        finally {
            await this.#client.close(context);
        }
    }
    httpEndpoints() {
        return this.endpoints().map((endpoint) => {
            if (!(endpoint instanceof NodeHttpSinkEndpoint)) {
                throw new Error(`sink endpoint ${endpoint.name} is not a Node HTTP endpoint`);
            }
            return endpoint;
        });
    }
    async stopEndpoints(context) {
        await Promise.all(this.httpEndpoints().map(async (endpoint) => endpoint.stop(context)));
    }
}
class NodeHttpSinkEndpointConsumer {
    #base;
    #streamContext;
    #handler;
    #client;
    #tracer;
    #tasks = new RuntimeTaskRegistry();
    #started = false;
    #stopped = false;
    constructor(endpoint, stream, client, handler) {
        this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
        this.#streamContext = new StreamContext(stream);
        this.#client = client;
        this.#handler = handler;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    start(context) {
        void context;
        if (this.#started) {
            return Promise.reject(new Error(`HTTP sink endpoint ${this.endpoint().name} is already started`));
        }
        if (this.#stopped) {
            return Promise.reject(new Error(`HTTP sink endpoint ${this.endpoint().name} is stopped`));
        }
        this.#started = true;
        return Promise.resolve();
    }
    async stop(context) {
        if (!this.#started) {
            return;
        }
        this.#started = false;
        this.#stopped = true;
        this.#tasks.stopAdmission();
        await drainAcceptedTasks(this.#tasks, context);
    }
    consume(context, value) {
        if (!this.#started) {
            return Promise.resolve();
        }
        return this.#tasks.admit(async (lifecycleSignal) => this.consumeOnce(context.withExternalCancellation(lifecycleSignal), value), context.signal());
    }
    async consumeOnce(context, value) {
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "http.output", [
                stringAttribute("stream", this.#base.stream().name),
                stringAttribute("endpoint", this.endpoint().name)
            ]);
            context = started.context;
            span = started.span;
        }
        let handlerContext;
        let handlerState;
        try {
            const started = await this.#handler.beginRequest(context, this.#streamContext);
            handlerContext = started.context;
            handlerState = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            this.endpoint().onBeginRequestFailed(context, failure);
            span?.end();
            return;
        }
        span?.addEvent("begin_request");
        const requestStarted = this.endpoint().onRequestStart(handlerContext);
        let requestError;
        let response;
        let errorEvent = "consume_message.error";
        try {
            const requester = new Requester();
            await this.#handler.consumeMessage(handlerContext, this.#streamContext, handlerState, value, requester);
            span?.addEvent("consume_message");
            const request = requester.request();
            if (request === undefined) {
                errorEvent = "no_request.error";
                throw new Error(`no HTTP request set by handler for sink endpoint ${this.endpoint().name}`);
            }
            const requestContext = request.context.withStreamId(newStreamId());
            const outgoingRequest = new Request(requestContext, request.method, request.url, request.body);
            for (const [name, value] of request.headers) {
                outgoingRequest.headers.set(name, value);
            }
            for (const [name, metadata] of requestContext.transportMetadata()) {
                outgoingRequest.headers.set(name, metadata);
            }
            errorEvent = "http_call.error";
            response = await this.#client.do(outgoingRequest);
            span?.addEvent("http_call", [int64Attribute("status_code", BigInt(response.statusCode))]);
            errorEvent = "handle_response.error";
            await this.#handler.handleResponse(handlerContext, this.#streamContext, handlerState, response);
            span?.addEvent("handle_response");
        }
        catch (error) {
            requestError = errorFromUnknown(error);
            spanError(span, requestError);
            span?.addEvent(errorEvent, [stringAttribute("error", requestError.message)]);
        }
        finally {
            if (response !== undefined) {
                try {
                    await response.close();
                }
                catch (error) {
                    requestError ??= errorFromUnknown(error);
                }
            }
            try {
                await this.#handler.endRequest(handlerContext, this.#streamContext, requestError, handlerState);
            }
            finally {
                try {
                    this.endpoint().onRequestEnd(handlerContext, requestStarted, requestError);
                }
                finally {
                    span?.end();
                }
            }
        }
    }
}
export function makeNodeHttpEndpointConsumer(stream, client, handler) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = requireHttpEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, client);
    if (dataSink.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new NodeHttpSinkEndpoint(dataSink, endpointConfig.id);
    const consumer = new NodeHttpSinkEndpointConsumer(endpoint, stream, client, handler);
    endpoint.bindConsumer(consumer);
    dataSink.addEndpoint(endpoint);
    stream.setSinkConsumer(consumer);
    return consumer;
}
function getOrCreateDataSink(connectorId, environment, client) {
    const existing = environment.dataSinkById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof NodeHttpDataSink)) {
            throw new Error(`data sink ${String(connectorId)} is not a Node HTTP data sink`);
        }
        if (existing.client() !== client) {
            throw new Error(`HTTP data sink ${existing.name} already uses a different client`);
        }
        return existing;
    }
    const dataSink = new NodeHttpDataSink(connectorId, environment, client);
    environment.addDataSink(dataSink);
    return dataSink;
}
function contextError(context, fallback) {
    return context.signal().reason === undefined
        ? new Error(fallback)
        : errorFromUnknown(context.signal().reason);
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
//# sourceMappingURL=node-http.js.map