import { Client, credentials, Metadata } from "@grpc/grpc-js";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { DataSinkEndpoint, DataSinkEndpointConsumerWithResult, FunctionCollector, OutputDataSink, SinkStreamContext, err, errorFromUnknown, int64Attribute, newStreamId, requireGrpcDataConnectorConfig, requireGrpcEndpointConfig, spanError, stringAttribute } from "../../runtime/index.js";
class RequestSender {
    request;
    send(_context, request) {
        this.request = request;
    }
}
class StreamSender {
    #write;
    #tail = Promise.resolve();
    #active = true;
    #span;
    constructor(write, span) {
        this.#write = write;
        this.#span = span;
    }
    send(_context, request) {
        if (!this.#active) {
            const error = new Error("gRPC request stream is closed");
            spanError(this.#span, error);
            this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
            return Promise.reject(error);
        }
        const delivery = this.#tail.then(() => new Promise((resolve, reject) => {
            this.#write(request, (error) => {
                if (error === undefined || error === null) {
                    this.#span?.addEvent("send");
                    resolve();
                }
                else {
                    spanError(this.#span, error);
                    this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
                    reject(error);
                }
            });
        }));
        this.#tail = delivery.catch(() => undefined);
        return delivery;
    }
    async close(close) {
        this.#active = false;
        await this.#tail;
        close();
    }
}
class StreamingResultContext {
    #done;
    #resolve;
    #span;
    constructor(span) {
        this.#span = span;
        this.#done = new Promise((resolve) => {
            this.#resolve = resolve;
        });
    }
    done() {
        if (this.#resolve === undefined)
            return;
        this.#span?.addEvent("done_called");
        this.#resolve();
        this.#resolve = undefined;
    }
    wait(signal) {
        if (signal.aborted)
            return Promise.reject(errorFromUnknown(signal.reason));
        let rejectCancellation;
        const cancellation = new Promise((_resolve, reject) => {
            rejectCancellation = reject;
        });
        const onAbort = () => {
            rejectCancellation?.(errorFromUnknown(signal.reason));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        return Promise.race([this.#done, cancellation]).finally(() => {
            signal.removeEventListener("abort", onAbort);
        });
    }
}
const unaryResultContext = { done: () => undefined };
class GrpcJsDataSink extends OutputDataSink {
    #service;
    #clients;
    #nextClient = 0;
    #started = false;
    #tasks = new Set();
    constructor(connectorId, environment, service) {
        super(connectorId, environment);
        const config = requireGrpcDataConnectorConfig(this.config());
        if (config.address === undefined || config.address.length === 0)
            throw new Error(`gRPC data connector ${config.name} has no address`);
        const address = config.address;
        this.#service = service;
        this.#clients = Array.from({ length: config.connectionsCount }, () => new Client(address, credentials.createInsecure(), {
            "grpc.use_local_subchannel_pool": 1
        }));
    }
    service() {
        return this.#service;
    }
    start(context) {
        void context;
        if (this.#started)
            return Promise.reject(new Error(`gRPC data sink ${this.name} already started`));
        this.#started = true;
        return Promise.resolve();
    }
    async stop(context) {
        if (this.#started) {
            this.#started = false;
            const drain = Promise.allSettled([...this.#tasks]).then(() => undefined);
            const remainingMs = context.remainingMs();
            if (remainingMs === undefined) {
                await drain;
            }
            else {
                let timer;
                try {
                    await Promise.race([
                        drain,
                        new Promise((resolve) => {
                            timer = setTimeout(resolve, remainingMs);
                        })
                    ]);
                }
                finally {
                    if (timer !== undefined)
                        clearTimeout(timer);
                }
            }
            for (const client of this.#clients)
                client.close();
            await Promise.allSettled([...this.#tasks]);
        }
    }
    track(context, task) {
        const observed = task
            .catch((error) => {
            this.runtimeEnvironment()
                .log()
                .error(context, "gRPC background task failed", err(errorFromUnknown(error)));
        })
            .finally(() => {
            this.#tasks.delete(observed);
        });
        this.#tasks.add(observed);
    }
    unary(context, method, request) {
        const metadata = metadataFromContext(context);
        const remainingMs = context.remainingMs();
        return new Promise((resolve, reject) => {
            const call = this.nextClient().makeUnaryRequest(`/${this.#service.typeName}/${method.name}`, (value) => Buffer.from(serialize(method.input, value)), (bytes) => deserialize(method.output, bytes), request, metadata, remainingMs === undefined ? {} : { deadline: Date.now() + remainingMs }, (error, response) => {
                context.signal().removeEventListener("abort", cancel);
                if (error !== null)
                    reject(error);
                else if (response === undefined)
                    reject(new Error("unary gRPC call returned no response"));
                else
                    resolve(response);
            });
            const cancel = () => {
                call.cancel();
            };
            if (context.cancelled())
                cancel();
            else
                context.signal().addEventListener("abort", cancel, { once: true });
        });
    }
    serverStream(context, method, request) {
        const call = this.nextClient().makeServerStreamRequest(`/${this.#service.typeName}/${method.name}`, (value) => Buffer.from(serialize(method.input, value)), (bytes) => deserialize(method.output, bytes), request, metadataFromContext(context), callOptions(context));
        bindCancellation(context, call);
        return call;
    }
    clientStream(context, method) {
        let call;
        const response = new Promise((resolve, reject) => {
            call = this.nextClient().makeClientStreamRequest(`/${this.#service.typeName}/${method.name}`, (value) => Buffer.from(serialize(method.input, value)), (bytes) => deserialize(method.output, bytes), metadataFromContext(context), callOptions(context), (error, value) => {
                if (error !== null)
                    reject(error);
                else if (value === undefined)
                    reject(new Error("client-streaming gRPC call returned no response"));
                else
                    resolve(value);
            });
        });
        if (call === undefined)
            throw new Error("gRPC client stream was not created");
        bindCancellation(context, call);
        return [call, response];
    }
    bidiStream(context, method) {
        const call = this.nextClient().makeBidiStreamRequest(`/${this.#service.typeName}/${method.name}`, (value) => Buffer.from(serialize(method.input, value)), (bytes) => deserialize(method.output, bytes), metadataFromContext(context), callOptions(context));
        bindCancellation(context, call);
        return call;
    }
    nextClient() {
        const client = this.#clients[this.#nextClient];
        if (client === undefined)
            throw new Error(`gRPC data sink ${this.name} has no clients`);
        this.#nextClient = (this.#nextClient + 1) % this.#clients.length;
        return client;
    }
}
class GrpcUnaryEndpointConsumer {
    #base;
    #streamContext;
    #handler;
    #method;
    #tracer;
    constructor(endpoint, stream, method, handler) {
        this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
        this.#streamContext = new SinkStreamContext(stream, stream.runtimeEnvironment().log(), new FunctionCollector((context, value) => stream.consumeResult(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        this.#method = method;
        this.#handler = handler;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    async consume(context, value) {
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "grpc.output", [
                stringAttribute("stream", this.#base.stream().name),
                stringAttribute("endpoint", this.endpoint().name)
            ]);
            context = started.context;
            span = started.span;
        }
        let state;
        let handlerContext;
        try {
            const starting = this.#handler.beginRequest(context, this.#streamContext);
            const started = starting instanceof Promise ? await starting : starting;
            state = started.state;
            handlerContext = started.context;
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
        const startedAt = this.endpoint().onRequestStart(handlerContext);
        let failure;
        let phase = "consume_message";
        try {
            const sender = new RequestSender();
            const consuming = this.#handler.consumeMessage(handlerContext, this.#streamContext, state, value, sender, unaryResultContext);
            if (consuming !== undefined)
                await consuming;
            span?.addEvent("consume_message");
            if (sender.request === undefined)
                throw new Error("gRPC sink handler produced no request");
            const dataSink = this.endpoint().dataSink();
            if (!(dataSink instanceof GrpcJsDataSink))
                throw new Error("invalid gRPC data sink");
            phase = "grpc_call";
            const requestContext = handlerContext.withStreamId(newStreamId());
            const response = await dataSink.unary(requestContext, this.#method, sender.request);
            span?.addEvent("grpc_call");
            phase = "handle_response";
            const handling = this.#handler.handleResponse(handlerContext, this.#streamContext, state, response);
            if (handling !== undefined)
                await handling;
            span?.addEvent("handle_response");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
        }
        finally {
            try {
                const ending = this.#handler.endRequest(handlerContext, this.#streamContext, failure, state);
                if (ending !== undefined)
                    await ending;
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            finally {
                try {
                    this.endpoint().onRequestEnd(handlerContext, startedAt, failure);
                }
                finally {
                    span?.end();
                }
            }
        }
    }
}
class GrpcServerStreamingEndpointConsumer {
    #base;
    #streamContext;
    #handler;
    #method;
    #tracer;
    constructor(endpoint, stream, method, handler) {
        this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
        this.#streamContext = makeSinkContext(stream);
        this.#method = method;
        this.#handler = handler;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    async consume(context, value) {
        const traced = startOutputSpan(context, this.#base, this.#tracer);
        context = traced.context;
        const { span } = traced;
        let state;
        try {
            const started = await this.#handler.beginRequest(context, this.#streamContext);
            context = started.context;
            state = started.state;
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
        const startedAt = this.endpoint().onRequestStart(context);
        let failure;
        let phase = "consume_message";
        try {
            const sender = new RequestSender();
            await this.#handler.consumeMessage(context, this.#streamContext, state, value, sender, unaryResultContext);
            span?.addEvent("consume_message");
            if (sender.request === undefined)
                throw new Error("gRPC sink handler produced no request");
            const dataSink = requireGrpcJsDataSink(this.endpoint());
            phase = "grpc_call";
            const requestContext = context.withStreamId(newStreamId());
            const call = dataSink.serverStream(requestContext, this.#method, sender.request);
            span?.addEvent("grpc_call");
            const responses = call;
            let messageCount = 0;
            phase = "recv";
            for await (const response of responses) {
                phase = "handle_response";
                await this.#handler.handleResponse(context, this.#streamContext, state, response);
                messageCount += 1;
                phase = "recv";
            }
            span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
        }
        catch (error) {
            failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
        }
        finally {
            try {
                await this.#handler.endRequest(context, this.#streamContext, failure, state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            finally {
                try {
                    this.endpoint().onRequestEnd(context, startedAt, failure);
                }
                finally {
                    span?.end();
                }
            }
        }
    }
}
class GrpcClientStreamingEndpointConsumer {
    #base;
    #streamContext;
    #handler;
    #method;
    #tracer;
    #pending = new Map();
    constructor(endpoint, stream, method, handler) {
        this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
        this.#streamContext = makeSinkContext(stream);
        this.#method = method;
        this.#handler = handler;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    async consume(context, value) {
        const streamId = context.streamId() ?? newStreamId();
        context = context.withStreamId(streamId);
        let sessionPromise = this.#pending.get(streamId);
        if (sessionPromise === undefined) {
            sessionPromise = this.createSession(context, streamId);
            this.#pending.set(streamId, sessionPromise);
        }
        let session;
        try {
            session = await sessionPromise;
        }
        catch {
            return;
        }
        const consume = session.consumeTail.then(async () => {
            try {
                await this.#handler.consumeMessage(session.context, this.#streamContext, session.state, value, session.sender, session.result);
                session.span?.addEvent("consume_message");
            }
            catch (error) {
                const failure = errorFromUnknown(error);
                spanError(session.span, failure);
                session.span?.addEvent("consume_message.error", [
                    stringAttribute("error", failure.message)
                ]);
                throw failure;
            }
        });
        session.consumeTail = consume.catch(() => undefined);
        try {
            await consume;
        }
        catch {
            session.result.done();
        }
    }
    async createSession(context, streamId) {
        let state;
        try {
            const started = await this.#handler.beginRequest(context, this.#streamContext);
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            this.endpoint().onBeginRequestFailed(context, failure);
            this.#pending.delete(streamId);
            throw failure;
        }
        const traced = startOutputSpan(context, this.#base, this.#tracer);
        context = traced.context;
        const { span } = traced;
        const startedAt = this.endpoint().onRequestStart(context);
        span?.addEvent("begin_request");
        const phase = "grpc_call";
        try {
            const dataSink = requireGrpcJsDataSink(this.endpoint());
            const requestContext = context.withStreamId(newStreamId());
            const [call, response] = dataSink.clientStream(requestContext, this.#method);
            span?.addEvent("grpc_call");
            const sender = new StreamSender((request, callback) => call.write(request, callback), span);
            const result = new StreamingResultContext(span);
            const session = {
                context,
                state,
                sender,
                result,
                span,
                consumeTail: Promise.resolve()
            };
            dataSink.track(context, this.finishSession(streamId, session, response, () => call.end(), startedAt, span));
            return session;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
            this.#pending.delete(streamId);
            await this.#handler.endRequest(context, this.#streamContext, failure, state);
            this.endpoint().onRequestEnd(context, startedAt, failure);
            span?.end();
            throw failure;
        }
    }
    async finishSession(streamId, session, response, close, startedAt, span) {
        let failure;
        let phase = "close_and_recv";
        try {
            await session.result.wait(session.context.signal());
            await session.consumeTail;
            await session.sender.close(close);
            const received = await response;
            span?.addEvent("close_and_recv");
            phase = "handle_response";
            await this.#handler.handleResponse(session.context, this.#streamContext, session.state, received);
            span?.addEvent("handle_response");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
        }
        finally {
            this.#pending.delete(streamId);
            try {
                await this.#handler.endRequest(session.context, this.#streamContext, failure, session.state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            finally {
                try {
                    this.endpoint().onRequestEnd(session.context, startedAt, failure);
                }
                finally {
                    span?.end();
                }
            }
        }
    }
}
class GrpcBidiStreamingEndpointConsumer {
    #base;
    #streamContext;
    #handler;
    #method;
    #tracer;
    #pending = new Map();
    constructor(endpoint, stream, method, handler) {
        this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
        this.#streamContext = makeSinkContext(stream);
        this.#method = method;
        this.#handler = handler;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    async consume(context, value) {
        const streamId = context.streamId() ?? newStreamId();
        context = context.withStreamId(streamId);
        let sessionPromise = this.#pending.get(streamId);
        if (sessionPromise === undefined) {
            sessionPromise = this.createSession(context, streamId);
            this.#pending.set(streamId, sessionPromise);
        }
        let session;
        try {
            session = await sessionPromise;
        }
        catch {
            return;
        }
        const consume = session.consumeTail.then(async () => {
            try {
                await this.#handler.consumeMessage(session.context, this.#streamContext, session.state, value, session.sender, session.result);
                session.span?.addEvent("consume_message");
            }
            catch (error) {
                const failure = errorFromUnknown(error);
                spanError(session.span, failure);
                session.span?.addEvent("consume_message.error", [
                    stringAttribute("error", failure.message)
                ]);
                throw failure;
            }
        });
        session.consumeTail = consume.catch(() => undefined);
        try {
            await consume;
        }
        catch {
            session.result.done();
        }
    }
    async createSession(context, streamId) {
        let state;
        try {
            const started = await this.#handler.beginRequest(context, this.#streamContext);
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            this.endpoint().onBeginRequestFailed(context, failure);
            this.#pending.delete(streamId);
            throw failure;
        }
        const traced = startOutputSpan(context, this.#base, this.#tracer);
        context = traced.context;
        const { span } = traced;
        const startedAt = this.endpoint().onRequestStart(context);
        span?.addEvent("begin_request");
        const phase = "grpc_call";
        try {
            const dataSink = requireGrpcJsDataSink(this.endpoint());
            const requestContext = context.withStreamId(newStreamId());
            const call = dataSink.bidiStream(requestContext, this.#method);
            span?.addEvent("grpc_call");
            const sender = new StreamSender((request, callback) => call.write(request, callback), span);
            const result = new StreamingResultContext(span);
            const session = {
                context,
                state,
                sender,
                result,
                span,
                consumeTail: Promise.resolve()
            };
            dataSink.track(context, this.finishSession(streamId, session, call, startedAt, span));
            return session;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
            this.#pending.delete(streamId);
            await this.#handler.endRequest(context, this.#streamContext, failure, state);
            this.endpoint().onRequestEnd(context, startedAt, failure);
            span?.end();
            throw failure;
        }
    }
    async finishSession(streamId, session, call, startedAt, span) {
        let failure;
        const receive = this.receiveResponses(session, call);
        try {
            const winner = await Promise.race([
                session.result.wait(session.context.signal()).then(() => "done"),
                receive.then(() => "responses")
            ]);
            await session.consumeTail;
            await session.sender.close(() => call.end());
            if (winner === "done")
                await receive;
            span?.addEvent("done_received");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            spanError(span, failure);
            call.cancel();
        }
        finally {
            this.#pending.delete(streamId);
            try {
                await this.#handler.endRequest(session.context, this.#streamContext, failure, session.state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            finally {
                try {
                    this.endpoint().onRequestEnd(session.context, startedAt, failure);
                }
                finally {
                    span?.end();
                }
            }
        }
    }
    async receiveResponses(session, call) {
        const responses = call;
        let messageCount = 0;
        for await (const response of responses) {
            await this.#handler.handleResponse(session.context, this.#streamContext, session.state, response);
            messageCount += 1;
        }
        session.span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
    }
}
export function makeGrpcNoStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "unary")
        throw new Error(`gRPC method ${method.name} is not unary`);
    const environment = stream.runtimeEnvironment();
    const endpointConfig = requireGrpcEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, service);
    if (dataSink.endpoint(endpointConfig.id) !== undefined)
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    const endpoint = new DataSinkEndpoint(dataSink, endpointConfig.id);
    const consumer = new GrpcUnaryEndpointConsumer(endpoint, stream, method, handler);
    endpoint.addEndpointConsumer(consumer);
    dataSink.addEndpoint(endpoint);
    stream.setSinkConsumer(consumer);
    return consumer;
}
export function makeGrpcServerStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "server_streaming")
        throw new Error(`gRPC method ${method.name} is not server-streaming`);
    const [dataSink, endpoint] = createSinkEndpoint(stream, service);
    const consumer = new GrpcServerStreamingEndpointConsumer(endpoint, stream, method, handler);
    bindSinkEndpoint(dataSink, endpoint, stream, consumer);
    return consumer;
}
export function makeGrpcClientStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "client_streaming")
        throw new Error(`gRPC method ${method.name} is not client-streaming`);
    const [dataSink, endpoint] = createSinkEndpoint(stream, service);
    const consumer = new GrpcClientStreamingEndpointConsumer(endpoint, stream, method, handler);
    bindSinkEndpoint(dataSink, endpoint, stream, consumer);
    return consumer;
}
export function makeGrpcBidiStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "bidi_streaming")
        throw new Error(`gRPC method ${method.name} is not bidirectional-streaming`);
    const [dataSink, endpoint] = createSinkEndpoint(stream, service);
    const consumer = new GrpcBidiStreamingEndpointConsumer(endpoint, stream, method, handler);
    bindSinkEndpoint(dataSink, endpoint, stream, consumer);
    return consumer;
}
function createSinkEndpoint(stream, service) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = requireGrpcEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, service);
    if (dataSink.endpoint(endpointConfig.id) !== undefined)
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    return [dataSink, new DataSinkEndpoint(dataSink, endpointConfig.id)];
}
function bindSinkEndpoint(dataSink, endpoint, stream, consumer) {
    endpoint.addEndpointConsumer(consumer);
    dataSink.addEndpoint(endpoint);
    stream.setSinkConsumer(consumer);
}
function makeSinkContext(stream) {
    return new SinkStreamContext(stream, stream.runtimeEnvironment().log(), new FunctionCollector((context, value) => stream.consumeResult(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
}
function startOutputSpan(context, base, tracer) {
    if (tracer === undefined || !context.samplingEnabled())
        return { context, span: undefined };
    return tracer.start(context, "grpc.output", [
        stringAttribute("stream", base.stream().name),
        stringAttribute("endpoint", base.endpoint().name)
    ]);
}
function requireGrpcJsDataSink(endpoint) {
    const dataSink = endpoint.dataSink();
    if (!(dataSink instanceof GrpcJsDataSink))
        throw new Error("invalid gRPC data sink");
    return dataSink;
}
function getOrCreateDataSink(connectorId, environment, service) {
    const existing = environment.dataSinkById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof GrpcJsDataSink))
            throw new Error(`data sink ${String(connectorId)} is not gRPC`);
        if (existing.service() !== service)
            throw new Error(`gRPC data sink ${existing.name} uses another service descriptor`);
        return existing;
    }
    const sink = new GrpcJsDataSink(connectorId, environment, service);
    environment.addDataSink(sink);
    return sink;
}
function metadataFromContext(context) {
    const metadata = new Metadata();
    for (const [key, value] of context.transportMetadata())
        metadata.set(key, value);
    return metadata;
}
function callOptions(context) {
    const remainingMs = context.remainingMs();
    return remainingMs === undefined ? {} : { deadline: Date.now() + remainingMs };
}
function bindCancellation(context, call) {
    const cancel = () => {
        call.cancel();
    };
    if (context.cancelled())
        cancel();
    else {
        context.signal().addEventListener("abort", cancel, { once: true });
        call.once("status", () => {
            context.signal().removeEventListener("abort", cancel);
        });
    }
}
function serialize(schema, value) {
    return toBinary(schema, value);
}
function deserialize(schema, bytes) {
    return fromBinary(schema, bytes);
}
//# sourceMappingURL=grpc-js.js.map