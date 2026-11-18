import { PublicationMetrics } from "../../internal/broker-publication-metrics.js";
import { headers } from "@nats-io/transport-node";
import { OutputDataSink, DataSinkEndpoint, MessageContextKey, requireNATSDataConnectorConfig, requireNATSEndpointConfig, validateNATSEndpoint, errorFromUnknown, err } from "../../runtime/index.js";
import { cancellation, messageBytes, openConnection, resources, wait, PENDING_MESSAGES, PENDING_BYTES } from "../../internal/nats/client.js";
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
            this.controller.abort(new Error("NATS request finalized"));
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
    data = new Uint8Array();
    header = headers();
    constructor(ep, output) {
        this.ep = ep;
        this.output = output;
    }
    subject() {
        return requireNATSEndpointConfig(this.ep.config()).subject;
    }
    /** Returns after bounded admission; callback follows native publication outcome. */
    send(context, onDelivery) {
        if (!this.#active) {
            this.ep.publication.observe(context, "error");
            throw new Error("NATS SinkMessage cannot be retained after ConsumeMessage");
        }
        this.ep.send(context, this.data, this.header, async (delivery, error) => {
            const result = onDelivery(delivery, error);
            if (result !== undefined)
                await this.output(context, result);
        });
    }
    /** Await the same native outcome; does not emit a graph result implicitly. */
    sendSync(context) {
        if (!this.#active) {
            this.ep.publication.observe(context, "error");
            return Promise.reject(new Error("NATS SinkMessage cannot be retained after ConsumeMessage"));
        }
        return this.ep.sendSync(context, this.data, this.header);
    }
    out(context, result) {
        return this.output(context, result);
    }
    skip(context, result) {
        return this.out(context, result);
    }
}
class SinkEndpoint extends DataSinkEndpoint {
    publication = new PublicationMetrics(this.runtimeEnvironment().metrics(), this.dataSink().config().type === 8 ? "jetstream" : "nats", this.dataSink().name, this.name);
    #connection;
    #js;
    #controller = new AbortController();
    #owner;
    #accepting = false;
    #started = false;
    #stopped = false;
    #connected = false;
    #tasks = new Set();
    #handlers = new Set();
    #bytes = 0;
    #timeout = 5000;
    async start(parent) {
        if (this.#started || this.#stopped)
            throw new Error("NATS sink already started or stopped");
        this.#started = true;
        const ep = requireNATSEndpointConfig(this.config());
        if (!ep.enabled)
            return;
        const dc = requireNATSDataConnectorConfig(this.dataSink().config());
        validateNATSEndpoint(ep, dc.type, true);
        this.#timeout = dc.operationTimeoutMs;
        const owner = parent.withExternalCancellation(this.#controller.signal);
        this.#owner = owner;
        const nc = await openConnection(dc, owner);
        this.#connection = nc;
        try {
            if (dc.type === 8)
                this.#js = (await wait(resources(nc, ep, dc.operationTimeoutMs, false), owner, dc.operationTimeoutMs)).js;
            if (this.stopped() || owner.cancelled())
                throw new Error("NATS sink stopped during startup");
            this.#connected = true;
            this.#accepting = true;
            void nc.closed().then((error) => {
                this.#connected = false;
                this.#accepting = false;
                if (error)
                    this.report(owner, error);
            });
            void (async () => {
                for await (const status of nc.status()) {
                    if (status.type === "disconnect" || status.type === "reconnecting")
                        this.#connected = false;
                    if (status.type === "reconnect")
                        this.#connected = true;
                    if (status.type === "error")
                        this.report(owner, status.error);
                }
            })().catch((error) => {
                this.report(owner, errorFromUnknown(error));
            });
        }
        catch (error) {
            await nc.close();
            throw error;
        }
    }
    stopped() {
        return this.#stopped;
    }
    report(context, error) {
        this.runtimeEnvironment().log().error(context, "NATS sink failure", err(error));
    }
    runHandler(context, handler) {
        if (!this.#accepting || !this.#owner)
            return Promise.reject(new Error("NATS sink is not accepting requests"));
        if (this.#handlers.size >= PENDING_MESSAGES)
            return Promise.reject(new Error("NATS sink handler admission limit exceeded"));
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
    reserve(context, data, header) {
        // Admitted handlers may publish while shutdown is draining; new handlers cannot enter.
        if (!this.#connection ||
            !this.#owner ||
            this.#controller.signal.aborted ||
            !this.#connected ||
            this.#connection.isClosed())
            throw new Error("NATS sink connection unavailable");
        const owner = context;
        if (owner.cancelled() || this.#owner.cancelled())
            throw cancellation(owner);
        const copy = headers();
        for (const [key, values] of header)
            for (const value of values)
                copy.append(key, value);
        for (const [key, value] of context.transportMetadata()) {
            for (const existing of copy.keys())
                if (existing.toLowerCase() === key)
                    copy.delete(existing);
            copy.set(key, value);
        }
        const bytes = messageBytes(data, this.subject(), copy);
        if (this.#tasks.size >= PENDING_MESSAGES || this.#bytes + bytes > PENDING_BYTES)
            throw new Error("NATS sink admission limit exceeded: 256 publications / 8 MiB");
        this.#bytes += bytes;
        return { context: owner, data: data.slice(), header: copy, bytes };
    }
    subject() {
        return requireNATSEndpointConfig(this.config()).subject;
    }
    publish(context, data, header) {
        const nc = this.#connection;
        if (!nc)
            return Promise.reject(new Error("NATS connection unavailable"));
        const config = requireNATSEndpointConfig(this.config());
        if (context.cancelled())
            return Promise.reject(cancellation(context));
        // Both paths use the official SDK asynchronously. Cancellation is an ambiguous
        // outcome after transmission, never a claim that a publication was withdrawn.
        const operation = this.#js
            ? this.#js
                .publish(config.subject, data, {
                headers: header,
                timeout: this.#timeout,
                expect: { ...(config.streamName ? { streamName: config.streamName } : {}) },
                retries: 1 // SDK counts total attempts; do not retry ambiguous publications.
            })
                .then((ack) => ({ ack }))
            : (() => {
                nc.publish(config.subject, data, { headers: header });
                return nc.flush().then(() => ({ ack: undefined }));
            })();
        return wait(operation, context, this.#timeout);
    }
    send(context, data, header, callback) {
        try {
            let reserved;
            try {
                reserved = this.reserve(context, data, header);
            }
            catch (error) {
                throw errorFromUnknown(error);
            }
            const { context: owner, data: payload, header: copied, bytes } = reserved;
            const scope = context.localValue(requestScope);
            scope?.retain();
            // Deferring execution one microtask makes reservation visible before callbacks
            // or reentrant sends run; no unbounded detached callback queue.
            const task = Promise.resolve().then(async () => {
                let delivery;
                let failure;
                try {
                    delivery = await this.publish(owner, payload, copied);
                }
                catch (error) {
                    failure = errorFromUnknown(error);
                }
                this.publication.observe(context, failure ? "error" : delivery?.ack ? "acknowledged" : "flushed");
                try {
                    await callback(delivery, failure);
                }
                finally {
                    // A timed-out/cancelled SDK publication can still be in reconnect buffers.
                    // Keep its count/byte reservation until an actual flush or connection close;
                    // otherwise repeated timeouts could grow native buffers without bound.
                    if (failure && this.#connection && this.#owner && !this.#connection.isClosed())
                        await wait(this.#connection.flush(), this.#owner);
                }
            });
            this.#tasks.add(task);
            void task.then(() => {
                this.#tasks.delete(task);
                this.#bytes -= bytes;
                scope?.release();
            }, (error) => {
                this.#tasks.delete(task);
                this.#bytes -= bytes;
                scope?.release();
                this.report(context, errorFromUnknown(error));
            });
        }
        catch (error) {
            this.publication.observe(context, "error");
            throw error;
        }
    }
    sendSync(context, data, header) {
        return new Promise((resolve, reject) => {
            try {
                this.send(context, data, header, (delivery, error) => {
                    if (error)
                        reject(error);
                    else if (delivery)
                        resolve(delivery);
                    else
                        reject(new Error("NATS publication had no outcome"));
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
        catch (error) {
            this.report(context, errorFromUnknown(error));
            throw error;
        }
        finally {
            this.#controller.abort(new Error("NATS sink stopped"));
            await this.#connection?.close();
        }
    }
}
export class NATSDataSink extends OutputDataSink {
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
    const config = requireNATSEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const existing = environment.dataSinkById(config.idDataConnector);
    if (existing && !(existing instanceof NATSDataSink))
        throw new Error("connector already has a different sink adapter");
    const sink = existing ?? new NATSDataSink(config.idDataConnector, environment);
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
            const handlerContext = begun.context.withExternalCancellation(owner.signal());
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
export function makeNATSEndpointConsumer(stream, handler) {
    return makeConsumer(stream, handler, () => {
        throw new Error("NATS sink has no result stream");
    });
}
export function makeNATSEndpointConsumerWithResult(stream, handler) {
    return makeConsumer(stream, handler, (context, value) => stream.consumeResult(context, value));
}
//# sourceMappingURL=index.js.map