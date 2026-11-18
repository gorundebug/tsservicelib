import { PublicationMetrics } from "../../internal/broker-publication-metrics.js";
import { OutputDataSink, DataSinkEndpoint, MessageContextKey, requireRedisStreamsDataConnectorConfig, requireRedisStreamsEndpointConfig, validateRedisStreamsEndpoint, errorFromUnknown, err } from "../../runtime/index.js";
import { cancellation, wait, Session, snapshot, BYTE_LIMIT } from "../../internal/redis/client.js";
/** Typed native sink context; plain sinks expose errors without inventing result streams. */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export class StreamContext {
    stream;
    output;
    errors;
    logger;
    constructor(stream, output, errors) {
        this.stream = stream;
        this.output = output;
        this.errors = errors;
        this.logger = stream.runtimeEnvironment().log();
    }
    collect(context, value) {
        return this.output(context, value);
    }
    errorCollect(context, value) {
        return this.errors(context, value);
    }
}
class RequestScope {
    controller = new AbortController();
    #references = 1;
    retain() {
        this.#references++;
    }
    release() {
        if (--this.#references === 0)
            this.controller.abort(new Error("RedisStreams request finalized"));
    }
}
const requestScope = new MessageContextKey(undefined);
export class SinkMessage {
    ep;
    output;
    #active = true;
    retire() {
        this.#active = false;
    }
    fields = [];
    constructor(ep, output) {
        this.ep = ep;
        this.output = output;
    }
    /** Returns after bounded admission; callback follows native publication outcome. */
    send(context, onDelivery) {
        if (!this.#active) {
            this.ep.publication.observe(context, "not-sent");
            throw new Error("RedisStreams SinkMessage cannot be retained after ConsumeMessage");
        }
        this.ep.send(context, this.fields, async (delivery, error) => {
            const result = await onDelivery(delivery, error);
            if (result !== undefined)
                await this.output(context, result);
        });
    }
    /** Await the same native outcome; does not emit a graph result implicitly. */
    sendSync(context) {
        if (!this.#active) {
            this.ep.publication.observe(context, "not-sent");
            return Promise.reject(new Error("RedisStreams SinkMessage cannot be retained after ConsumeMessage"));
        }
        return this.ep.sendSync(context, this.fields);
    }
    out(context, result) {
        return this.output(context, result);
    }
    skip(context, result) {
        return this.out(context, result);
    }
}
class SinkEndpoint extends DataSinkEndpoint {
    publication = new PublicationMetrics(this.runtimeEnvironment().metrics(), "redisstreams", this.dataSink().name, this.name);
    #session;
    #controller = new AbortController();
    #owner;
    #accepting = false;
    #started = false;
    #stopped = false;
    #tasks = new Set();
    #handlers = new Set();
    #bytes = 0;
    async start(parent) {
        if (this.#started || this.#stopped)
            throw new Error("RedisStreams sink already started/stopped");
        this.#started = true;
        const ep = requireRedisStreamsEndpointConfig(this.config());
        if (!ep.enabled)
            return;
        const dc = requireRedisStreamsDataConnectorConfig(this.dataSink().config());
        validateRedisStreamsEndpoint(ep, true);
        const owner = parent.withExternalCancellation(this.#controller.signal);
        this.#owner = owner;
        this.#session = new Session(dc, ep, false, (error) => {
            this.report(owner, errorFromUnknown(error));
        });
        await this.#session.start(owner);
        this.#accepting = true;
    }
    report(context, error) {
        this.runtimeEnvironment().log().error(context, "RedisStreams sink failure", err(error));
    }
    runHandler(context, handler) {
        if (!this.#accepting || !this.#owner)
            return Promise.reject(new Error("RedisStreams sink is not accepting requests"));
        if (this.#handlers.size >= (this.#session?.limit ?? 256))
            return Promise.reject(new Error("RedisStreams sink handler admission limit exceeded"));
        const scope = new RequestScope();
        const owner = context
            .withExternalCancellation(scope.controller.signal)
            .withExternalCancellation(this.#owner.signal())
            .withLocalValue(requestScope, scope);
        if (owner.cancelled()) {
            scope.release();
            return Promise.reject(cancellation(owner));
        }
        const task = Promise.resolve()
            .then(() => handler(owner))
            .finally(() => {
            scope.release();
        });
        this.#handlers.add(task);
        void task.then(() => this.#handlers.delete(task), () => this.#handlers.delete(task));
        return task;
    }
    send(context, fields, callback) {
        try {
            const session = this.#session;
            if (!session?.ready || this.#controller.signal.aborted)
                throw new Error("RedisStreams connection unavailable");
            if (context.cancelled())
                throw cancellation(context);
            const copied = snapshot(fields, context);
            if (this.#tasks.size >= session.limit || this.#bytes + copied.bytes > BYTE_LIMIT)
                throw new Error("RedisStreams publication count/byte admission limit exceeded");
            this.#bytes += copied.bytes;
            const scope = context.localValue(requestScope);
            scope?.retain();
            const task = Promise.resolve().then(async () => {
                let delivery;
                try {
                    delivery = await session.publish(copied.fields, context);
                }
                catch {
                    delivery = { outcome: "unknown", error: new Error("RedisStreams publication failed") };
                }
                this.publication.observe(context, delivery.outcome);
                await callback(delivery, delivery.error);
            });
            this.#tasks.add(task);
            void task.then(() => {
                finish();
            }, (error) => {
                finish();
                this.report(context, errorFromUnknown(error));
            });
            const finish = () => {
                this.#tasks.delete(task);
                this.#bytes -= copied.bytes;
                scope?.release();
            };
        }
        catch (error) {
            this.publication.observe(context, "not-sent");
            throw error;
        }
    }
    sendSync(context, fields) {
        return new Promise((resolve, reject) => {
            try {
                this.send(context, fields, (delivery) => {
                    resolve(delivery ?? { outcome: "unknown" });
                });
            }
            catch (error) {
                reject(errorFromUnknown(error));
            }
        });
    }
    stopAdmission() {
        this.#stopped = true;
        this.#accepting = false;
    }
    async stop(context) {
        this.stopAdmission();
        try {
            await wait((async () => {
                while (this.#handlers.size || this.#tasks.size)
                    await Promise.allSettled([...this.#handlers, ...this.#tasks]);
            })(), context);
        }
        finally {
            this.#session?.close();
            this.#controller.abort(new Error("RedisStreams sink stopped"));
        }
    }
}
export class RedisStreamsDataSink extends OutputDataSink {
    constructor(id, environment) {
        super(id, environment);
    }
    async start(context) {
        try {
            for (const endpoint of this.endpoints())
                await endpoint.start(context);
        }
        catch (error) {
            await this.stop(context.bounded(1000)).catch(() => undefined);
            throw error;
        }
    }
    async stop(context) {
        for (const endpoint of this.endpoints())
            endpoint.stopAdmission();
        const results = await Promise.allSettled(this.endpoints().map((endpoint) => endpoint.stop(context)));
        const failure = results.find((result) => result.status === "rejected");
        if (failure?.status === "rejected")
            throw errorFromUnknown(failure.reason);
    }
}
function makeConsumer(stream, handler, collect) {
    const environment = stream.runtimeEnvironment();
    const config = requireRedisStreamsEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const existing = environment.dataSinkById(config.idDataConnector);
    if (existing && !(existing instanceof RedisStreamsDataSink))
        throw new Error("connector already has a different sink adapter");
    const sink = existing ?? new RedisStreamsDataSink(config.idDataConnector, environment);
    if (!existing)
        environment.addDataSink(sink);
    const oldEndpoint = sink.endpoint(config.id);
    if (oldEndpoint && !(oldEndpoint instanceof SinkEndpoint))
        throw new Error("endpoint already uses a different sink adapter");
    const ep = oldEndpoint ?? new SinkEndpoint(sink, config.id);
    if (!oldEndpoint)
        sink.addEndpoint(ep);
    const sc = new StreamContext(stream, collect, (context, value) => stream.errorStream().consume(context, value));
    const consumer = {
        consume: (context, value) => ep.runHandler(context, async (owner) => {
            const request = owner.withStreamId(handler.getStreamId(owner, value));
            let begun;
            try {
                begun = await handler.beginRequest(request, sc);
            }
            catch (error) {
                ep.onBeginRequestFailed(request, errorFromUnknown(error));
                return;
            }
            const handlerContext = begun.context
                .withExternalCancellation(owner.signal())
                .withLocalValue(requestScope, owner.localValue(requestScope));
            const start = ep.onRequestStart(handlerContext);
            let failure;
            const message = new SinkMessage(ep, collect);
            try {
                await handler.consumeMessage(handlerContext, sc, begun.state, value, message);
            }
            catch (error) {
                failure = errorFromUnknown(error);
            }
            finally {
                message.retire();
            }
            try {
                await handler.endRequest(handlerContext, sc, failure, begun.state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                ep.report(handlerContext, failure);
            }
            finally {
                ep.onRequestEnd(handlerContext, start, failure);
            }
        })
    };
    stream.setSinkConsumer(consumer);
    ep.addEndpointConsumer({ endpoint: () => ep });
    return consumer;
}
export function makeRedisStreamsEndpointConsumer(stream, handler) {
    return makeConsumer(stream, handler, () => {
        throw new Error("RedisStreams sink has no result stream");
    });
}
export function makeRedisStreamsEndpointConsumerWithResult(stream, handler) {
    return makeConsumer(stream, handler, (context, value) => stream.consumeResult(context, value));
}
//# sourceMappingURL=index.js.map