import { Server, ServerCredentials } from "@grpc/grpc-js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { DataSourceEndpoint, DataSourceEndpointConsumer, FunctionCollector, InputDataSource, Context, MessageContext, STREAM_ID_HEADER, TRACE_SAMPLING_HEADER, errorFromUnknown, err, boolAttribute, int64Attribute, makeStreamContext, newStreamId, requireGrpcDataConnectorConfig, requireGrpcEndpointConfig, spanError, str, stringAttribute } from "../../runtime/index.js";
class GrpcJsDataSource extends InputDataSource {
    #services = new Map();
    #server;
    constructor(connectorId, environment) {
        super(connectorId, environment);
        requireGrpcDataConnectorConfig(this.config());
    }
    add(service, method, handler) {
        let methods = this.#services.get(service);
        if (methods === undefined) {
            methods = new Map();
            this.#services.set(service, methods);
        }
        if (methods.has(method.localName))
            throw new Error(`gRPC method ${method.name} already bound`);
        methods.set(method.localName, handler);
    }
    async start(context) {
        void context;
        if (this.#server !== undefined)
            throw new Error(`gRPC data source ${this.name} already started`);
        const server = new Server();
        for (const [service, handlers] of this.#services) {
            server.addService(serviceDefinition(service), Object.fromEntries(handlers));
        }
        const config = this.runtimeEnvironment().serviceConfig();
        await new Promise((resolve, reject) => {
            server.bindAsync(`${config.grpcHost}:${String(config.grpcPort)}`, ServerCredentials.createInsecure(), (error) => {
                if (error === null)
                    resolve();
                else
                    reject(error);
            });
        });
        this.#server = server;
    }
    async stop(context) {
        const server = this.#server;
        this.#server = undefined;
        if (server === undefined)
            return;
        await new Promise((resolve) => {
            const timeout = context.remainingMs();
            const timer = timeout === undefined
                ? undefined
                : setTimeout(() => {
                    server.forceShutdown();
                    resolve();
                }, timeout);
            server.tryShutdown(() => {
                if (timer !== undefined)
                    clearTimeout(timer);
                resolve();
            });
        });
    }
}
class RequestResult {
    #callbacks = new Map();
    #span;
    #recordDone;
    #done;
    #resolve;
    #completed = false;
    #retiring = false;
    #activeCallbacks = 0;
    #retired;
    #resolveRetired;
    constructor(span, recordDone) {
        this.#span = span;
        this.#recordDone = recordDone;
    }
    setResultCallback(messageId, callback) {
        this.#callbacks.set(messageId, callback);
    }
    callback(messageId) {
        return this.#callbacks.get(messageId);
    }
    remove(messageId, callback) {
        if (this.#callbacks.get(messageId) !== callback)
            return false;
        return this.#callbacks.delete(messageId);
    }
    done() {
        if (this.#completed)
            return;
        this.#completed = true;
        if (this.#recordDone)
            this.#span?.addEvent("done_called");
        this.#resolve?.();
        this.#resolve = undefined;
    }
    wait() {
        if (this.#completed)
            return Promise.resolve();
        this.#done ??= new Promise((resolve) => {
            this.#resolve = resolve;
        });
        return this.#done;
    }
    completed() {
        return this.#completed;
    }
    span() {
        return this.#span;
    }
    beginCallback() {
        if (this.#retiring)
            return false;
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
        return this.#completed;
    }
}
class UnarySender {
    #send;
    #span;
    #rejectDuplicate;
    #sent = false;
    constructor(send, span, rejectDuplicate = true) {
        this.#send = send;
        this.#span = span;
        this.#rejectDuplicate = rejectDuplicate;
    }
    send(_context, value) {
        if (this.#sent) {
            if (!this.#rejectDuplicate)
                return;
            const error = new Error("unary gRPC response already sent");
            spanError(this.#span, error);
            this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
            throw error;
        }
        this.#sent = true;
        this.#span?.addEvent("send");
        this.#send(value);
    }
}
class StreamingSender {
    #write;
    #tail = Promise.resolve();
    #active = true;
    #span;
    constructor(write, span) {
        this.#write = write;
        this.#span = span;
    }
    send(_context, value) {
        if (!this.#active) {
            const error = new Error("stream is closed");
            spanError(this.#span, error);
            this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
            return Promise.reject(error);
        }
        const delivery = this.#tail.then(() => new Promise((resolve, reject) => {
            this.#write(value, (error) => {
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
    async close() {
        this.#active = false;
        await this.#tail;
    }
}
function consumePendingResult(context, value, pendingRequests, handler, streamContext, endpoint) {
    const streamId = context.streamId();
    if (streamId === undefined) {
        endpoint.onMissingStreamId(context);
        return;
    }
    const pending = pendingRequests.get(streamId);
    if (pending === undefined) {
        endpoint.onLateResult(context, streamId);
        return;
    }
    if (!pending.result.beginCallback()) {
        endpoint.onLateResult(context, streamId);
        pending.result.span()?.addEvent("late_result");
        return;
    }
    let asynchronous = false;
    try {
        const messageId = handler.getMessageId(context, streamContext, pending.state, value);
        const callback = pending.result.callback(messageId);
        if (callback === undefined) {
            endpoint.onUnknownMessageId(context, streamId, messageId);
            pending.result
                .span()
                ?.addEvent("unknown_message_id", [stringAttribute("message_id", messageId)]);
            return;
        }
        const finish = (remove) => {
            if (remove && !pending.result.remove(messageId, callback)) {
                endpoint.onDuplicateMessageId(context, streamId, messageId);
                pending.result
                    .span()
                    ?.addEvent("duplicate_message_id", [stringAttribute("message_id", messageId)]);
            }
            pending.result
                .span()
                ?.addEvent("result_consumed", [stringAttribute("message_id", messageId)]);
        };
        const consumed = callback(context, streamContext, pending.state, value, pending.sender);
        if (consumed instanceof Promise) {
            asynchronous = true;
            return consumed.then(finish).finally(() => {
                pending.result.endCallback();
            });
        }
        finish(consumed);
    }
    finally {
        if (!asynchronous)
            pending.result.endCallback();
    }
}
function observeGrpcHandler(handler, endpoint, failTransport) {
    void handler.catch((value) => {
        const failure = errorFromUnknown(value);
        endpoint
            .runtimeEnvironment()
            .log()
            .error(Context.background(), "gRPC request handler failed", str("endpoint", endpoint.name), err(failure));
        try {
            failTransport(failure);
        }
        catch (transportError) {
            endpoint
                .runtimeEnvironment()
                .log()
                .error(Context.background(), "gRPC request failure reporting failed", str("endpoint", endpoint.name), err(errorFromUnknown(transportError)));
        }
    });
}
class GrpcStreamingSourceConsumer extends DataSourceEndpointConsumer {
    handler;
    streamContext;
    pending = new Map();
    tracer;
    constructor(endpoint, stream, handler) {
        super(endpoint, stream);
        this.handler = handler;
        this.streamContext = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((context, value) => stream.consume(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        if (stream.resultStream() !== undefined) {
            stream.setResultConsumer({ consume: (context, value) => this.consumeResult(context, value) });
        }
        this.tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    hasResult() {
        return this.stream().resultStream() !== undefined;
    }
    requestContext(call) {
        let context = contextFromCall(call);
        let span;
        if (this.tracer !== undefined && context.samplingEnabled()) {
            const started = this.tracer.start(context, "grpc.input", [
                stringAttribute("stream", this.stream().name),
                stringAttribute("endpoint", this.endpoint().name)
            ]);
            context = started.context;
            span = started.span;
        }
        return { context, span };
    }
    addPending(context, state, result, sender) {
        const streamId = context.streamId() ?? newStreamId();
        if (this.pending.has(streamId))
            throw new Error("duplicate key");
        this.pending.set(streamId, { state, result, sender });
        this.endpoint().onPendingAdd(context, streamId);
        return streamId;
    }
    removePending(context, streamId) {
        if (this.pending.delete(streamId))
            this.endpoint().onPendingRemove(context, streamId);
    }
    consumeResult(context, value) {
        return consumePendingResult(context, value, this.pending, this.handler, this.streamContext, this.endpoint());
    }
}
class GrpcUnaryEndpointConsumer extends DataSourceEndpointConsumer {
    #handler;
    #streamContext;
    #pending = new Map();
    #tracer;
    constructor(endpoint, stream, handler) {
        super(endpoint, stream);
        this.#handler = handler;
        this.#streamContext = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((context, value) => stream.consume(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        stream.setResultConsumer({ consume: (context, value) => this.consumeResult(context, value) });
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    handle() {
        return (call, callback) => {
            let completed = false;
            const complete = (error, value) => {
                if (completed)
                    return;
                completed = true;
                callback(error, value);
            };
            observeGrpcHandler(this.handleCall(call, complete), this.endpoint(), (failure) => {
                complete(failure);
            });
        };
    }
    async handleCall(call, callback) {
        let context = contextFromCall(call);
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "grpc.input", [
                stringAttribute("stream", this.stream().name),
                stringAttribute("endpoint", this.endpoint().name)
            ]);
            context = started.context;
            span = started.span;
        }
        let state;
        try {
            const starting = this.#handler.beginRequest(context, this.#streamContext);
            const started = starting instanceof Promise ? await starting : starting;
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            this.endpoint().onBeginRequestFailed(context, failure);
            callback(failure);
            span?.end();
            return;
        }
        span?.addEvent("begin_request");
        const startedAt = this.endpoint().onRequestStart(context);
        const streamId = context.streamId() ?? newStreamId();
        context = context.withStreamId(streamId);
        const hasResult = this.stream().resultStream() !== undefined;
        span?.setAttributes([
            stringAttribute("stream_id", streamId),
            boolAttribute("has_result", hasResult)
        ]);
        const result = new RequestResult(span, false);
        let response;
        const sender = new UnarySender((value) => {
            response = value;
            result.done();
        }, span);
        let failure;
        let resultWaitFailed = false;
        let phase = "consume_message";
        if (hasResult) {
            if (this.#pending.has(streamId)) {
                failure = new Error("duplicate key");
                const ending = this.#handler.endRequest(context, this.#streamContext, failure, state);
                if (ending !== undefined)
                    await ending;
                this.endpoint().onRequestEnd(context, startedAt, failure);
                callback(failure);
                span?.end();
                return;
            }
            this.#pending.set(streamId, { state, result, sender });
            this.endpoint().onPendingAdd(context, streamId);
        }
        try {
            const consuming = this.#handler.consumeMessage(context, this.#streamContext, state, call.request, result, sender);
            if (consuming !== undefined)
                await consuming;
            span?.addEvent("consume_message");
            phase = "eof";
            const eof = this.#handler.eof(context, this.#streamContext, state);
            if (eof !== undefined)
                await eof;
            span?.addEvent("eof");
            if (hasResult) {
                phase = "result";
                const waiting = waitForResult(result, context);
                const waitFailure = waiting instanceof Promise ? await waiting : waiting;
                if (waitFailure !== undefined) {
                    resultWaitFailed = true;
                    throw waitFailure;
                }
                span?.addEvent("result_received");
            }
        }
        catch (error) {
            failure = errorFromUnknown(error);
            if (phase === "consume_message") {
                span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
            }
            else if (phase === "result") {
                span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
            }
        }
        finally {
            if (hasResult) {
                const resultCompleted = await result.retire();
                if (resultWaitFailed && resultCompleted)
                    failure = undefined;
                this.#pending.delete(streamId);
                this.endpoint().onPendingRemove(context, streamId);
            }
            if (failure !== undefined)
                spanError(span, failure);
            try {
                const ending = this.#handler.endRequest(context, this.#streamContext, failure, state);
                if (ending !== undefined)
                    await ending;
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            try {
                this.endpoint().onRequestEnd(context, startedAt, failure);
            }
            finally {
                span?.end();
            }
        }
        if (failure !== undefined)
            callback(failure);
        else if (response === undefined)
            callback(new Error("unary gRPC handler produced no response"));
        else
            callback(null, response);
    }
    consumeResult(context, value) {
        return consumePendingResult(context, value, this.#pending, this.#handler, this.#streamContext, this.endpoint());
    }
}
class GrpcClientStreamingEndpointConsumer extends GrpcStreamingSourceConsumer {
    #method;
    constructor(endpoint, stream, method, handler) {
        super(endpoint, stream, handler);
        this.#method = method;
    }
    handle() {
        return (call, callback) => {
            let completed = false;
            const complete = (error, value) => {
                if (completed)
                    return;
                completed = true;
                callback(error, value);
            };
            observeGrpcHandler(this.handleCall(call, complete), this.endpoint(), (failure) => {
                complete(failure);
            });
        };
    }
    async handleCall(call, callback) {
        const request = this.requestContext(call);
        let { context } = request;
        const { span } = request;
        let state;
        try {
            const started = await this.handler.beginRequest(context, this.streamContext);
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            this.endpoint().onBeginRequestFailed(context, failure);
            callback(failure);
            span?.end();
            return;
        }
        span?.addEvent("begin_request");
        const startedAt = this.endpoint().onRequestStart(context);
        const streamId = context.streamId() ?? newStreamId();
        context = context.withStreamId(streamId);
        const hasResult = this.hasResult();
        span?.setAttributes([
            stringAttribute("stream_id", streamId),
            boolAttribute("has_result", hasResult)
        ]);
        const result = new RequestResult(span, hasResult);
        let response;
        const sender = new UnarySender((value) => {
            response = value;
            result.done();
        }, span, false);
        let pending = false;
        let failure;
        let resultWaitFailed = false;
        let phase = "recv";
        try {
            if (hasResult) {
                this.addPending(context, state, result, sender);
                pending = true;
            }
            const requests = call;
            let messageCount = 0;
            for await (const request of requests) {
                phase = "consume_message";
                await this.handler.consumeMessage(context, this.streamContext, state, request, result, sender);
                messageCount += 1;
                phase = "recv";
            }
            span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
            phase = "eof";
            await this.handler.eof(context, this.streamContext, state);
            if (hasResult) {
                phase = "result";
                const waiting = waitForResult(result, context);
                const waitFailure = waiting instanceof Promise ? await waiting : waiting;
                if (waitFailure !== undefined) {
                    resultWaitFailed = true;
                    throw waitFailure;
                }
                span?.addEvent("done_received");
            }
            if (!hasResult) {
                sender.send(context, create(this.#method.output));
            }
        }
        catch (error) {
            failure = errorFromUnknown(error);
            if (phase === "consume_message") {
                span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
            }
            else if (phase === "recv") {
                span?.addEvent("recv.error", [stringAttribute("error", failure.message)]);
            }
            else if (phase === "result") {
                span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
            }
        }
        finally {
            if (pending) {
                const resultCompleted = await result.retire();
                if (resultWaitFailed && resultCompleted)
                    failure = undefined;
                this.removePending(context, streamId);
            }
            if (failure !== undefined)
                spanError(span, failure);
            try {
                await this.handler.endRequest(context, this.streamContext, failure, state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            try {
                this.endpoint().onRequestEnd(context, startedAt, failure);
            }
            finally {
                span?.end();
            }
        }
        if (failure !== undefined)
            callback(failure);
        else
            callback(null, response);
    }
}
class GrpcServerStreamingEndpointConsumer extends GrpcStreamingSourceConsumer {
    handle() {
        return (call) => {
            observeGrpcHandler(this.handleCall(call), this.endpoint(), (failure) => {
                call.destroy(failure);
            });
        };
    }
    async handleCall(call) {
        const request = this.requestContext(call);
        let { context } = request;
        const { span } = request;
        const sender = new StreamingSender((value, callback) => call.write(value, callback), span);
        let state;
        try {
            const started = await this.handler.beginRequest(context, this.streamContext);
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            this.endpoint().onBeginRequestFailed(context, failure);
            await sender.close();
            call.destroy(failure);
            span?.end();
            return;
        }
        span?.addEvent("begin_request");
        const startedAt = this.endpoint().onRequestStart(context);
        const streamId = context.streamId() ?? newStreamId();
        context = context.withStreamId(streamId);
        const hasResult = this.hasResult();
        span?.setAttributes([
            stringAttribute("stream_id", streamId),
            boolAttribute("has_result", hasResult)
        ]);
        const result = new RequestResult(span, hasResult);
        let pending = false;
        let failure;
        let resultWaitFailed = false;
        let phase = "consume_message";
        try {
            if (hasResult) {
                this.addPending(context, state, result, sender);
                pending = true;
            }
            await this.handler.consumeMessage(context, this.streamContext, state, call.request, result, sender);
            span?.addEvent("consume_message");
            phase = "eof";
            await this.handler.eof(context, this.streamContext, state);
            span?.addEvent("eof");
            if (hasResult) {
                phase = "result";
                const waiting = waitForResult(result, context);
                const waitFailure = waiting instanceof Promise ? await waiting : waiting;
                if (waitFailure !== undefined) {
                    resultWaitFailed = true;
                    throw waitFailure;
                }
                span?.addEvent("done_received");
            }
        }
        catch (error) {
            failure = errorFromUnknown(error);
            if (phase === "consume_message") {
                span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
            }
            else if (phase === "result") {
                span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
            }
        }
        finally {
            if (pending) {
                const resultCompleted = await result.retire();
                if (resultWaitFailed && resultCompleted)
                    failure = undefined;
                this.removePending(context, streamId);
            }
            if (failure !== undefined)
                spanError(span, failure);
            try {
                await this.handler.endRequest(context, this.streamContext, failure, state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            await sender.close();
            try {
                this.endpoint().onRequestEnd(context, startedAt, failure);
            }
            finally {
                span?.end();
            }
        }
        if (failure === undefined)
            call.end();
        else
            call.destroy(failure);
    }
}
class GrpcBidiStreamingEndpointConsumer extends GrpcStreamingSourceConsumer {
    handle() {
        return (call) => {
            observeGrpcHandler(this.handleCall(call), this.endpoint(), (failure) => {
                call.destroy(failure);
            });
        };
    }
    async handleCall(call) {
        const request = this.requestContext(call);
        let { context } = request;
        const { span } = request;
        const sender = new StreamingSender((value, callback) => call.write(value, callback), span);
        let state;
        try {
            const started = await this.handler.beginRequest(context, this.streamContext);
            context = started.context;
            state = started.state;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            this.endpoint().onBeginRequestFailed(context, failure);
            await sender.close();
            call.destroy(failure);
            span?.end();
            return;
        }
        span?.addEvent("begin_request");
        const startedAt = this.endpoint().onRequestStart(context);
        const streamId = context.streamId() ?? newStreamId();
        context = context.withStreamId(streamId);
        const hasResult = this.hasResult();
        span?.setAttributes([
            stringAttribute("stream_id", streamId),
            boolAttribute("has_result", hasResult)
        ]);
        const result = new RequestResult(span, hasResult);
        let pending = false;
        let failure;
        let resultWaitFailed = false;
        let phase = "recv";
        try {
            if (hasResult) {
                this.addPending(context, state, result, sender);
                pending = true;
            }
            // Reading the request half must not destroy the duplex response half when
            // the client half-closes it. The response stream remains active through
            // eof/result delivery, exactly like the canonical bidirectional endpoint.
            const requests = call.iterator({ destroyOnReturn: false });
            let messageCount = 0;
            for await (const request of requests) {
                phase = "consume_message";
                await this.handler.consumeMessage(context, this.streamContext, state, request, result, sender);
                messageCount += 1;
                phase = "recv";
            }
            span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
            phase = "eof";
            await this.handler.eof(context, this.streamContext, state);
            if (hasResult) {
                phase = "result";
                const waiting = waitForResult(result, context);
                const waitFailure = waiting instanceof Promise ? await waiting : waiting;
                if (waitFailure !== undefined) {
                    resultWaitFailed = true;
                    throw waitFailure;
                }
                span?.addEvent("done_received");
            }
        }
        catch (error) {
            failure = errorFromUnknown(error);
            if (phase === "consume_message") {
                span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
            }
            else if (phase === "recv") {
                span?.addEvent("recv.error", [stringAttribute("error", failure.message)]);
            }
            else if (phase === "result") {
                span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
            }
        }
        finally {
            if (pending) {
                const resultCompleted = await result.retire();
                if (resultWaitFailed && resultCompleted)
                    failure = undefined;
                this.removePending(context, streamId);
            }
            if (failure !== undefined)
                spanError(span, failure);
            try {
                await this.handler.endRequest(context, this.streamContext, failure, state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            await sender.close();
            try {
                this.endpoint().onRequestEnd(context, startedAt, failure);
            }
            finally {
                span?.end();
            }
        }
        if (failure === undefined)
            call.end();
        else
            call.destroy(failure);
    }
}
export function makeGrpcNoStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "unary")
        throw new Error(`gRPC method ${method.name} is not unary`);
    const environment = stream.runtimeEnvironment();
    const endpointConfig = requireGrpcEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
    if (dataSource.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new DataSourceEndpoint(dataSource, endpointConfig.id);
    const consumer = new GrpcUnaryEndpointConsumer(endpoint, stream, handler);
    endpoint.addEndpointConsumer(consumer);
    dataSource.addEndpoint(endpoint);
    dataSource.add(service, method, consumer.handle());
    return consumer;
}
export function makeGrpcClientStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "client_streaming")
        throw new Error(`gRPC method ${method.name} is not client-streaming`);
    const [dataSource, endpoint] = createSourceEndpoint(stream);
    const consumer = new GrpcClientStreamingEndpointConsumer(endpoint, stream, method, handler);
    bindSourceEndpoint(dataSource, endpoint, service, method, consumer, consumer.handle());
    return consumer;
}
export function makeGrpcServerStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "server_streaming")
        throw new Error(`gRPC method ${method.name} is not server-streaming`);
    const [dataSource, endpoint] = createSourceEndpoint(stream);
    const consumer = new GrpcServerStreamingEndpointConsumer(endpoint, stream, handler);
    bindSourceEndpoint(dataSource, endpoint, service, method, consumer, consumer.handle());
    return consumer;
}
export function makeGrpcBidiStreamingEndpointConsumer(stream, service, method, handler) {
    if (method.methodKind !== "bidi_streaming")
        throw new Error(`gRPC method ${method.name} is not bidirectional-streaming`);
    const [dataSource, endpoint] = createSourceEndpoint(stream);
    const consumer = new GrpcBidiStreamingEndpointConsumer(endpoint, stream, handler);
    bindSourceEndpoint(dataSource, endpoint, service, method, consumer, consumer.handle());
    return consumer;
}
function createSourceEndpoint(stream) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = requireGrpcEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
    if (dataSource.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new DataSourceEndpoint(dataSource, endpointConfig.id);
    return [dataSource, endpoint];
}
function bindSourceEndpoint(dataSource, endpoint, service, method, consumer, handler) {
    endpoint.addEndpointConsumer(consumer);
    dataSource.addEndpoint(endpoint);
    dataSource.add(service, method, handler);
}
function getOrCreateDataSource(connectorId, environment) {
    const existing = environment.dataSourceById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof GrpcJsDataSource))
            throw new Error(`data source ${String(connectorId)} is not gRPC`);
        return existing;
    }
    requireGrpcDataConnectorConfig(environment.runtimeConfig().dataConnectorById(connectorId));
    const source = new GrpcJsDataSource(connectorId, environment);
    environment.addDataSource(source);
    return source;
}
function contextFromMetadata(metadata) {
    const values = new Map();
    for (const key of [
        STREAM_ID_HEADER,
        TRACE_SAMPLING_HEADER,
        "traceparent",
        "tracestate",
        "baggage"
    ]) {
        const value = metadata.get(key)[0];
        if (value !== undefined) {
            values.set(key, typeof value === "string" ? value : value.toString("utf8"));
        }
    }
    if (!values.has(STREAM_ID_HEADER))
        values.set(STREAM_ID_HEADER, newStreamId());
    return new MessageContext().withMetadata(values);
}
function contextFromCall(call) {
    const controller = new AbortController();
    call.once("cancelled", () => {
        controller.abort(new Error("gRPC call cancelled"));
    });
    let context = contextFromMetadata(call.metadata).withExternalCancellation(controller.signal);
    const deadline = call.getDeadline();
    const deadlineTimestamp = deadline instanceof Date ? deadline.getTime() : deadline;
    if (Number.isFinite(deadlineTimestamp)) {
        context = context.bounded(Math.max(0, deadlineTimestamp - Date.now()));
    }
    return context;
}
function waitForResult(result, context) {
    if (context.cancelled())
        return errorFromUnknown(context.signal().reason ?? new Error("gRPC call cancelled"));
    if (result.completed())
        return undefined;
    return waitForResultCompletion(result, context);
}
async function waitForResultCompletion(result, context) {
    let cancelled;
    try {
        return await Promise.race([
            result.wait().then(() => undefined),
            new Promise((resolve) => {
                cancelled = () => {
                    resolve(errorFromUnknown(context.signal().reason ?? new Error("gRPC call cancelled")));
                };
                context.signal().addEventListener("abort", cancelled, { once: true });
            })
        ]);
    }
    finally {
        if (cancelled !== undefined)
            context.signal().removeEventListener("abort", cancelled);
    }
}
function serviceDefinition(service) {
    return Object.fromEntries(service.methods.map((method) => [
        method.localName,
        {
            path: `/${service.typeName}/${method.name}`,
            requestStream: method.methodKind === "client_streaming" || method.methodKind === "bidi_streaming",
            responseStream: method.methodKind === "server_streaming" || method.methodKind === "bidi_streaming",
            requestSerialize: (value) => Buffer.from(serialize(method.input, value)),
            requestDeserialize: (bytes) => deserialize(method.input, bytes),
            responseSerialize: (value) => Buffer.from(serialize(method.output, value)),
            responseDeserialize: (bytes) => deserialize(method.output, bytes)
        }
    ]));
}
function serialize(schema, value) {
    return toBinary(schema, value);
}
function deserialize(schema, bytes) {
    return fromBinary(schema, bytes);
}
//# sourceMappingURL=grpc-js.js.map