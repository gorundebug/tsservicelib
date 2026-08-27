import { applyDataSourceEndpointTracing, DataConnectorType, DataSourceEndpoint, DataSourceEndpointConsumer, FunctionCollector, InputDataSource, ScheduleBackend, bindDurableCallSpan, errorFromUnknown, makeScheduleTrigger, makeStreamContext, newStreamId, spanError, stringAttribute } from "../../runtime/index.js";
import { makeTemporalConnector } from "./connector.js";
class TemporalDataSource extends InputDataSource {
    constructor(connectorId, environment) {
        super(connectorId, environment);
        if (this.config().type !== DataConnectorType.Temporal) {
            throw new Error(`data source ${this.name} is not Temporal`);
        }
    }
    start(_context) {
        void _context;
        return Promise.resolve();
    }
    stop(_context) {
        void _context;
        return Promise.resolve();
    }
}
class DirectTemporalEndpointHandler {
    beginRequest(context) {
        return { context, state: undefined };
    }
    consumeMessage(context, stream, _state, value) {
        return stream.collect(context, value);
    }
    endRequest() { }
}
class TemporalEndpointConsumer extends DataSourceEndpointConsumer {
    #endpoint;
    #stream;
    #decode;
    #handler;
    #streamContext;
    #pending = new Map();
    #tracer;
    constructor(endpoint, stream, connector, decode, handler) {
        super(endpoint, stream);
        this.#endpoint = endpoint;
        this.#stream = stream;
        this.#decode = decode;
        this.#handler = handler;
        this.#streamContext = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((context, value) => stream.consume(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
        if (stream.resultStream() !== undefined) {
            stream.setResultConsumer({
                consume: (context, value) => {
                    this.consumeResult(context, value);
                }
            });
        }
        connector.registerEndpoint(endpoint.id, (envelope, context, cancellationSignal) => this.activate(envelope, context, cancellationSignal));
    }
    async activate(envelope, parent, cancellationSignal) {
        if (envelope.version !== 1 || envelope.endpointId !== this.#endpoint.id) {
            throw new Error(`invalid Temporal endpoint envelope for ${this.#endpoint.name}`);
        }
        let context = parent
            .withStreamId(envelope.streamId || newStreamId())
            .withPriority(envelope.priority);
        context = applyDataSourceEndpointTracing(context, this.#stream.runtimeEnvironment(), this.#endpoint.id);
        if (cancellationSignal !== undefined) {
            context = context.withExternalCancellation(cancellationSignal);
        }
        if (envelope.deadlineUnixMillis > 0) {
            context = context.bounded(Math.max(0, envelope.deadlineUnixMillis - Date.now()));
        }
        const streamId = context.streamId();
        if (streamId === undefined)
            throw new Error("Temporal endpoint stream ID was not created");
        let span;
        let durableSpan = false;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const startedSpan = this.#tracer.start(context, "temporal.input", [
                stringAttribute("stream", this.#stream.name),
                stringAttribute("endpoint", this.#endpoint.name)
            ]);
            context = startedSpan.context;
            span = startedSpan.span;
            durableSpan = bindDurableCallSpan(context, span);
        }
        const startedHandler = await this.#handler.beginRequest(context, this.#streamContext);
        context = startedHandler.context;
        const state = startedHandler.state;
        const started = this.#endpoint.onRequestStart(context);
        const resultStream = this.#stream.resultStream();
        let pending;
        let failure;
        try {
            if (resultStream !== undefined) {
                if (this.#pending.has(streamId)) {
                    throw new Error(`Temporal execution ${streamId} is already active`);
                }
                pending = { ...Promise.withResolvers(), settled: false };
                this.#pending.set(streamId, pending);
                this.#endpoint.onPendingAdd(context, streamId);
            }
            await this.#handler.consumeMessage(context, this.#streamContext, state, this.#decode(envelope));
            if (pending === undefined)
                return { payload: new Uint8Array() };
            const value = await pending.promise;
            if (resultStream === undefined)
                throw new Error("Temporal endpoint result stream disappeared");
            return { payload: resultStream.serde().serialize(value) };
        }
        catch (error) {
            failure = errorFromUnknown(error);
            spanError(span, failure);
            throw failure;
        }
        finally {
            if (pending !== undefined) {
                this.#pending.delete(streamId);
                this.#endpoint.onPendingRemove(context, streamId);
            }
            await this.#handler.endRequest(context, this.#streamContext, failure, state);
            this.#endpoint.onRequestEnd(context, started, failure);
            if (!durableSpan)
                span?.end();
        }
    }
    consumeResult(context, value) {
        const streamId = context.streamId();
        if (streamId === undefined) {
            this.#endpoint.onMissingStreamId(context);
            return;
        }
        const pending = this.#pending.get(streamId);
        if (pending === undefined) {
            this.#endpoint.onLateResult(context, streamId);
            return;
        }
        if (pending.settled) {
            this.#endpoint.onDuplicateMessageId(context, streamId, streamId);
            return;
        }
        pending.settled = true;
        pending.resolve(value);
    }
}
export function makeTemporalEndpointConsumer(stream) {
    return makeTemporalEndpointConsumerWithHandler(stream, new DirectTemporalEndpointHandler());
}
export function makeTemporalEndpointConsumerWithHandler(stream, handler) {
    return makeEndpointConsumer(stream, (envelope) => stream.serde().deserialize(envelope.payload), handler);
}
export function makeTemporalScheduleEndpointConsumer(stream, function_) {
    return makeEndpointConsumer(stream, (envelope) => {
        if (!envelope.scheduled) {
            return { scheduled: false, value: stream.serde().deserialize(envelope.payload) };
        }
        if (envelope.scheduleId === "" ||
            envelope.scheduledAtUnixMillis <= 0 ||
            envelope.firedAtUnixMillis <= 0) {
            throw new Error(`invalid Temporal schedule envelope for ${String(envelope.endpointId)}`);
        }
        return {
            scheduled: true,
            trigger: makeScheduleTrigger(envelope.endpointId, envelope.scheduleId, new Date(envelope.scheduledAtUnixMillis).toISOString(), new Date(envelope.firedAtUnixMillis).toISOString(), ScheduleBackend.Temporal)
        };
    }, {
        beginRequest: (context) => ({ context, state: undefined }),
        consumeMessage: (context, streamContext, _state, activation) => activation.scheduled
            ? function_.onTrigger(context, activation.trigger, new FunctionCollector((nextContext, value) => streamContext.collect(nextContext, value)))
            : streamContext.collect(context, activation.value),
        endRequest: () => undefined
    });
}
function makeEndpointConsumer(stream, decode, handler) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
    if (endpointConfig === undefined) {
        throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
    }
    const connector = makeTemporalConnector(endpointConfig.idDataConnector, environment);
    const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
    if (dataSource.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`Temporal endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new DataSourceEndpoint(dataSource, endpointConfig.id);
    const consumer = new TemporalEndpointConsumer(endpoint, stream, connector, decode, handler);
    endpoint.addEndpointConsumer(consumer);
    dataSource.addEndpoint(endpoint);
    return consumer;
}
function getOrCreateDataSource(connectorId, environment) {
    const existing = environment.dataSourceById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof TemporalDataSource)) {
            throw new Error(`data source ${String(connectorId)} is not Temporal`);
        }
        return existing;
    }
    const dataSource = new TemporalDataSource(connectorId, environment);
    environment.addDataSource(dataSource);
    return dataSource;
}
//# sourceMappingURL=temporal.js.map