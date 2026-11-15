import { makeEndpointTraceAttributes } from "../../runtime/endpoint-tracing.js";
import { DataConnectorType, DataSinkEndpoint, OutputDataSink, errorFromUnknown, newStreamId, spanError } from "../../runtime/index.js";
import { makeTemporalConnector } from "../../datasource/temporal/connector.js";
class TemporalDataSink extends OutputDataSink {
    #active = new Set();
    constructor(connectorId, environment) {
        super(connectorId, environment);
        if (this.config().type !== DataConnectorType.Temporal) {
            throw new Error(`data sink ${this.name} is not Temporal`);
        }
    }
    start(_context) {
        void _context;
        return Promise.resolve();
    }
    stop(_context) {
        void _context;
        return this.#drain();
    }
    track(operation) {
        this.#active.add(operation);
        const remove = () => {
            this.#active.delete(operation);
        };
        void operation.then(remove, remove);
        return operation;
    }
    async #drain() {
        while (this.#active.size > 0) {
            await Promise.allSettled([...this.#active]);
        }
    }
}
class DirectTemporalEndpointHandler {
    beginRequest(context, stream) {
        void context;
        void stream;
        return Promise.resolve({});
    }
    getMessageId(context, stream, state, value) {
        void stream;
        void state;
        void value;
        return context.streamId() ?? "";
    }
    endRequest(context, stream, error, state) {
        void context;
        void stream;
        void error;
        void state;
        return Promise.resolve();
    }
}
class TemporalSinkConsumer {
    sinkEndpoint;
    dataSink;
    connector;
    stream;
    handler;
    withResult;
    options;
    #traceAttributes;
    #tracer;
    constructor(sinkEndpoint, dataSink, connector, stream, handler, withResult, options) {
        this.sinkEndpoint = sinkEndpoint;
        this.dataSink = dataSink;
        this.connector = connector;
        this.stream = stream;
        this.handler = handler;
        this.withResult = withResult;
        this.options = options;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
        this.#traceAttributes = makeEndpointTraceAttributes(stream, sinkEndpoint.name);
    }
    endpoint() {
        return this.sinkEndpoint;
    }
    async consume(context, value) {
        await this.dataSink.track(this.consumeTracked(context, value));
    }
    async consumeTracked(context, value) {
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const startedSpan = this.#tracer.start(context, "temporal.output", this.#traceAttributes);
            context = startedSpan.context;
            span = startedSpan.span;
        }
        const started = this.sinkEndpoint.onRequestStart(context);
        let failure;
        let state;
        let began = false;
        try {
            state = await this.handler.beginRequest(context, this.stream);
            began = true;
            const messageId = this.handler.getMessageId(context, this.stream, state, value) || newStreamId();
            const remainingMs = context.remainingMs();
            const envelope = {
                version: 1,
                endpointId: this.sinkEndpoint.id,
                messageId,
                streamId: context.streamId() ?? messageId,
                priority: context.priority() ?? 0,
                deadlineUnixMillis: remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
                scheduled: false,
                scheduleId: "",
                scheduledAtUnixMillis: 0,
                firedAtUnixMillis: 0,
                payload: this.stream.inputSerde().serialize(value)
            };
            const result = this.options === undefined
                ? await this.connector.submitEndpoint(context, this.sinkEndpoint.id, envelope, this.withResult)
                : await this.connector.submitEndpointWithOptions(context, this.sinkEndpoint.id, envelope, this.withResult, this.options(context, value));
            if (this.withResult) {
                const resultStream = this.stream;
                await resultStream.consumeResult(context, resultStream.serde().deserialize(result.payload));
            }
        }
        catch (error) {
            failure = errorFromUnknown(error);
            if (span !== undefined)
                spanError(span, failure);
            throw failure;
        }
        finally {
            if (began)
                await this.handler.endRequest(context, this.stream, failure, state);
            this.sinkEndpoint.onRequestEnd(context, started, failure);
            span?.end();
        }
    }
}
export function makeTemporalSinkEndpointConsumer(stream, options) {
    return makeTemporalSinkEndpointConsumerWithHandler(stream, new DirectTemporalEndpointHandler(), options);
}
export function makeTemporalSinkEndpointConsumerWithHandler(stream, handler, options) {
    return makeSinkConsumer(stream, handler, false, options);
}
export function makeTemporalSinkEndpointConsumerWithResult(stream, options) {
    return makeTemporalSinkEndpointConsumerWithResultHandler(stream, new DirectTemporalEndpointHandler(), options);
}
export function makeTemporalSinkEndpointConsumerWithResultHandler(stream, handler, options) {
    return makeSinkConsumer(stream, handler, true, options);
}
function makeSinkConsumer(stream, handler, withResult, options) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
    if (endpointConfig === undefined) {
        throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
    }
    const connector = makeTemporalConnector(endpointConfig.idDataConnector, environment);
    const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment);
    const existing = dataSink.endpoint(endpointConfig.id);
    if (existing !== undefined && !(existing instanceof DataSinkEndpoint)) {
        throw new Error(`Temporal endpoint ${endpointConfig.name} has an invalid runtime type`);
    }
    const endpoint = existing ?? new DataSinkEndpoint(dataSink, endpointConfig.id);
    const consumer = new TemporalSinkConsumer(endpoint, dataSink, connector, stream, handler, withResult, options);
    connector.registerEndpointSubmission(endpointConfig.id);
    endpoint.addEndpointConsumer(consumer);
    if (existing === undefined)
        dataSink.addEndpoint(endpoint);
    stream.setSinkConsumer(consumer);
    return consumer;
}
function getOrCreateDataSink(connectorId, environment) {
    const existing = environment.dataSinkById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof TemporalDataSink)) {
            throw new Error(`data sink ${String(connectorId)} is not Temporal`);
        }
        return existing;
    }
    const dataSink = new TemporalDataSink(connectorId, environment);
    environment.addDataSink(dataSink);
    return dataSink;
}
//# sourceMappingURL=temporal.js.map