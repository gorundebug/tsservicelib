import { DataConnectorType, DataSourceEndpoint, FunctionCollector, InputDataSource, MessageContext, ScheduleBackend, bindDurableCallSpan, errorFromUnknown, makeScheduleTrigger, newStreamId, spanError, stringAttribute } from "../../runtime/index.js";
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
class TemporalEndpointConsumer {
    #endpoint;
    #stream;
    #decode;
    #activateInput;
    #pending = new Map();
    #tracer;
    constructor(endpoint, stream, connector, decode, activateInput) {
        this.#endpoint = endpoint;
        this.#stream = stream;
        this.#decode = decode;
        this.#activateInput = activateInput;
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
        connector.registerEndpoint(endpoint.id, (envelope, cancellationSignal, durableCallContext) => this.activate(envelope, cancellationSignal, durableCallContext));
    }
    endpoint() {
        return this.#endpoint;
    }
    consume(context, value) {
        return this.#activateInput(context, value);
    }
    async activate(envelope, cancellationSignal, durableCallContext) {
        if (envelope.version !== 1 || envelope.endpointId !== this.#endpoint.id) {
            throw new Error(`invalid Temporal endpoint envelope for ${this.#endpoint.name}`);
        }
        let context = new MessageContext()
            .withMetadata(new Map(Object.entries(envelope.traceCarrier)))
            .withStreamId(envelope.streamId || newStreamId())
            .withPriority(envelope.priority)
            .withSampling(envelope.samplingEnabled);
        if (durableCallContext !== undefined) {
            context = context.withDurableCallContext(durableCallContext);
        }
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
        const started = this.#endpoint.onRequestStart(context);
        const expectsResult = this.#stream.resultStream() !== undefined;
        let pending;
        let failure;
        try {
            if (expectsResult) {
                if (this.#pending.has(streamId)) {
                    throw new Error(`Temporal execution ${streamId} is already active`);
                }
                pending = { ...Promise.withResolvers(), settled: false };
                this.#pending.set(streamId, pending);
                this.#endpoint.onPendingAdd(context, streamId);
            }
            await this.consume(context, this.#decode(envelope));
            if (pending === undefined)
                return { payload: new Uint8Array() };
            const value = await pending.promise;
            const resultStream = this.#stream.resultStream();
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
    return makeEndpointConsumer(stream, (envelope) => stream.serde().deserialize(envelope.payload), (context, value) => stream.consume(context, value));
}
export function makeTemporalScheduleEndpointConsumer(stream, function_) {
    const collector = new FunctionCollector((context, value) => stream.consume(context, value));
    return makeEndpointConsumer(stream, (envelope) => {
        if (!envelope.scheduled ||
            envelope.scheduleId === "" ||
            envelope.scheduledAtUnixMillis <= 0 ||
            envelope.firedAtUnixMillis <= 0) {
            throw new Error(`invalid Temporal schedule envelope for ${String(envelope.endpointId)}`);
        }
        return makeScheduleTrigger(envelope.endpointId, envelope.scheduleId, new Date(envelope.scheduledAtUnixMillis).toISOString(), new Date(envelope.firedAtUnixMillis).toISOString(), ScheduleBackend.Temporal);
    }, (context, trigger) => function_.onTrigger(context, trigger, collector));
}
function makeEndpointConsumer(stream, decode, activateInput) {
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
    const consumer = new TemporalEndpointConsumer(endpoint, stream, connector, decode, activateInput);
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