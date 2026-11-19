import { SourceMetrics } from "../../internal/broker-source-metrics.js";
import { serialize } from "node:v8";
import { InputDataSource, DataSourceEndpoint, DataSourceEndpointConsumer, MessageContext, MessageContextKey, FunctionCollector, makeStreamContext, newStreamId, applyDataSourceEndpointTracing, requireRabbitMQDataConnectorConfig, requireRabbitMQEndpointConfig, validateRabbitMQEndpoint, errorFromUnknown, err } from "../../runtime/index.js";
import { cancellation, Session, wait, PENDING_MESSAGES } from "../../internal/rabbitmq/client.js";
const settlementContexts = new WeakMap();
/** Settlement requests are local native operations, not broker-confirmed ACKs. */
export class ConsumerMessage {
    message;
    session;
    id;
    epoch;
    context;
    #active = true;
    #settled = false;
    constructor(message, session, id, epoch, context) {
        this.message = message;
        this.session = session;
        this.id = id;
        this.epoch = epoch;
        this.context = context;
    }
    retire() {
        this.#active = false;
    }
    async settle(action) {
        if (!this.#active || this.#settled)
            throw new Error("RabbitMQ delivery handle expired or already settled");
        this.#settled = true;
        await this.session.settle(this.id, this.epoch, action, settlementContexts.get(this) ?? this.context);
    }
    accept() {
        return this.settle("accept");
    }
    release() {
        return this.settle("release");
    }
    reject() {
        return this.settle("reject");
    }
}
const requestIdentity = new MessageContextKey(undefined);
class Request {
    state;
    identity = newStreamId();
    completed = false;
    retired = false;
    callbacks = new Map();
    active = new Set();
    completion;
    #finish;
    constructor(state) {
        this.state = state;
        this.completion = new Promise((resolve) => {
            this.#finish = resolve;
        });
    }
    done() {
        this.completed = true;
        this.#finish();
    }
    setResultCallback(id, callback) {
        if (this.retired)
            throw new Error("RabbitMQ request already retired");
        if (this.callbacks.size >= PENDING_MESSAGES)
            throw new Error("RabbitMQ result callback limit exceeded");
        this.callbacks.set(id, callback);
    }
}
class SourceEndpoint extends DataSourceEndpoint {
    observations = new SourceMetrics(this.runtimeEnvironment().metrics(), "rabbitmq", this.dataSource().name, this.name);
    handle;
    report;
    #session;
    #controller = new AbortController();
    #generation = new AbortController();
    #generationContext;
    #started = false;
    #stopping = false;
    #tasks = new Set();
    #bytes = 0;
    #reporting;
    async start(parent) {
        if (this.#started || this.#stopping)
            throw new Error("RabbitMQ source already started/stopped");
        this.#started = true;
        const ep = requireRabbitMQEndpointConfig(this.config());
        if (!ep.enabled)
            return;
        const dc = requireRabbitMQDataConnectorConfig(this.dataSource().config());
        validateRabbitMQEndpoint(ep, false);
        const owner = parent.withExternalCancellation(this.#controller.signal);
        // One cancellation join per transport generation, not per delivered record.
        this.#generationContext = owner.withExternalCancellation(this.#generation.signal);
        const session = new Session(dc, ep, true, (error) => {
            this.#generation.abort(error);
            this.#generation = new AbortController();
            this.#generationContext = owner.withExternalCancellation(this.#generation.signal);
            // At most one blocked error reporter is retained.
            if (!this.#reporting)
                this.#reporting = this.report(owner, error).finally(() => {
                    this.#reporting = undefined;
                });
        }, (native, id, epoch) => {
            if (this.#stopping)
                return;
            const size = serialize(native).length;
            if (this.#tasks.size >= session.limit || this.#bytes + size > 8 * 1024 * 1024) {
                session.invalidate(new Error("RabbitMQ active source request count/byte limit exhausted"));
                return;
            }
            this.#bytes += size;
            const context = this.#generationContext;
            const message = new ConsumerMessage(native, session, id, epoch, context);
            const task = Promise.resolve()
                .then(() => this.handle(context, message))
                .finally(() => {
                message.retire();
                this.#bytes -= size;
            });
            this.#tasks.add(task);
            void task.then(() => this.#tasks.delete(task), () => this.#tasks.delete(task));
        });
        this.#session = session;
        await session.start(owner);
    }
    stopInput() {
        this.#stopping = true;
        this.#session?.pause();
    }
    async stop(context) {
        this.stopInput();
        try {
            await wait(Promise.allSettled([...this.#tasks, ...(this.#reporting ? [this.#reporting] : [])]), context);
        }
        finally {
            const closing = this.#session?.close(context);
            this.#controller.abort(new Error("RabbitMQ source stopped"));
            await closing;
        }
    }
}
// Experimental: the TypeScript generator keeps RabbitMQ source.supported=false
// until the upstream receive-credit fix is released and integrated. See PR #94:
// https://github.com/coders51/rabbitmq-amqp-js-client/pull/94
// Integration requirements are documented beside createConsumer in worker.ts.
export class RabbitMQDataSource extends InputDataSource {
    constructor(id, environment) {
        super(id, environment);
    }
    async start(context) {
        try {
            for (const ep of this.endpoints())
                await ep.start(context);
        }
        catch (error) {
            await this.stop(context.bounded(1000)).catch(() => undefined);
            throw error;
        }
    }
    async stop(context) {
        for (const ep of this.endpoints())
            ep.stopInput();
        const results = await Promise.allSettled(this.endpoints().map((ep) => ep.stop(context)));
        const failure = results.find((result) => result.status === "rejected");
        if (failure?.status === "rejected")
            throw errorFromUnknown(failure.reason);
    }
}
class EndpointConsumer extends DataSourceEndpointConsumer {
    ep;
    handler;
    #pending = new Map();
    #sc;
    constructor(ep, stream, handler) {
        super(ep, stream);
        this.ep = ep;
        this.handler = handler;
        this.#sc = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((c, v) => stream.consume(c, v)), new FunctionCollector((c, v) => stream.errorStream().consume(c, v)));
        stream.setResultConsumer({ consume: (context, value) => this.result(context, value) });
        ep.handle = (context, message) => {
            ep.observations.observe(context, "received");
            if (ep.observations.enabled && (message.message.delivery_count ?? 0) > 0)
                ep.observations.observe(context, "redelivered");
            return this.handle(context, message);
        };
        ep.report = async (context, error) => {
            try {
                await handler.onError(MessageContext.fromContext(context), this.#sc, error);
            }
            catch (failure) {
                ep.runtimeEnvironment()
                    .log()
                    .error(context, "RabbitMQ onError failed", err(errorFromUnknown(failure)));
            }
        };
    }
    async handle(owner, message) {
        const requestCancel = new AbortController();
        let context = (owner instanceof MessageContext ? owner : MessageContext.fromContext(owner)).withExternalCancellation(requestCancel.signal);
        const metadata = new Map(context.metadata());
        for (const [key, value] of Object.entries(message.message.application_properties ?? {}))
            if (typeof value === "string")
                metadata.set(key.toLowerCase(), value);
        context = applyDataSourceEndpointTracing(context.withMetadata(metadata), this.ep.runtimeEnvironment(), this.ep.id);
        // Preserve correlation from the caller; fence delivery-local result ownership separately.
        context = context.withStreamId(context.streamId() ?? newStreamId());
        let state;
        try {
            const begun = await this.handler.beginRequest(context, this.#sc);
            context = begun.context.withExternalCancellation(owner.signal());
            state = begun.state;
        }
        catch (error) {
            this.ep.onBeginRequestFailed(context, errorFromUnknown(error));
            requestCancel.abort();
            return;
        }
        const id = context.streamId() ?? newStreamId();
        context = context.withStreamId(id);
        settlementContexts.set(message, context);
        const request = new Request(state);
        context = context.withLocalValue(requestIdentity, request.identity);
        const hasResult = this.stream().resultStream() !== undefined;
        const started = this.ep.onRequestStart(context);
        if (hasResult) {
            this.#pending.set(request.identity, request);
            this.ep.onPendingAdd(context, id);
        }
        let failure;
        let waitCancelled = false;
        try {
            await this.handler.consumeMessage(context, this.#sc, state, message, request);
            if (hasResult && !request.completed) {
                try {
                    await wait(request.completion, context);
                }
                catch (error) {
                    failure = errorFromUnknown(error);
                    waitCancelled = true;
                }
            }
            else if (!hasResult && context.cancelled())
                failure = cancellation(context);
        }
        catch (error) {
            failure = errorFromUnknown(error);
        }
        finally {
            request.retired = true;
            // Callbacks already running own state until they return; Stop still has a deadline.
            await Promise.allSettled([...request.active]);
            if (waitCancelled && request.completed)
                failure = undefined;
            this.#pending.delete(request.identity);
            request.callbacks.clear();
            if (hasResult)
                this.ep.onPendingRemove(context, id);
            try {
                await this.handler.endRequest(context, this.#sc, failure, state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                await this.ep.report(context, failure);
            }
            this.ep.onRequestEnd(context, started, failure);
            requestCancel.abort(new Error("RabbitMQ request finalized"));
        }
    }
    async result(context, value) {
        const id = context.streamId();
        if (!id) {
            this.ep.onMissingStreamId(context);
            return;
        }
        const identity = context.localValue(requestIdentity);
        const request = identity ? this.#pending.get(identity) : undefined;
        if (!request ||
            request.retired ||
            request.completed ||
            context.localValue(requestIdentity) !== request.identity) {
            this.ep.onLateResult(context, id);
            return;
        }
        if (request.active.size >= PENDING_MESSAGES)
            throw new Error("RabbitMQ active result callback limit exceeded");
        const operation = (async () => {
            const messageId = this.handler.getMessageId(context, this.#sc, request.state, value);
            const callback = request.callbacks.get(messageId);
            if (!callback) {
                this.ep.onUnknownMessageId(context, id, messageId);
                return;
            }
            if (await callback(context, this.#sc, request.state, value))
                request.callbacks.delete(messageId);
        })();
        request.active.add(operation);
        try {
            await operation;
        }
        finally {
            request.active.delete(operation);
        }
    }
}
export function makeRabbitMQEndpointConsumer(stream, handler) {
    const environment = stream.runtimeEnvironment();
    const config = requireRabbitMQEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const existing = environment.dataSourceById(config.idDataConnector);
    if (existing && !(existing instanceof RabbitMQDataSource))
        throw new Error("connector already has a different source adapter");
    const source = existing ?? new RabbitMQDataSource(config.idDataConnector, environment);
    if (!existing)
        environment.addDataSource(source);
    if (source.endpoint(config.id))
        throw new Error("duplicate RabbitMQ source endpoint");
    const ep = new SourceEndpoint(source, config.id);
    const consumer = new EndpointConsumer(ep, stream, handler);
    ep.addEndpointConsumer(consumer);
    source.addEndpoint(ep);
    return consumer;
}
//# sourceMappingURL=index.js.map