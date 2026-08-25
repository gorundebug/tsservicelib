import { DataConnectorType, DataSinkEndpoint, OutputDataSink, errorFromUnknown, newStreamId, spanError, stringAttribute } from "../../runtime/index.js";
import { makeTemporalConnector } from "../../datasource/temporal/connector.js";
class TemporalDataSink extends OutputDataSink {
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
        return Promise.resolve();
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
    connector;
    stream;
    handler;
    withResult;
    #tracer;
    constructor(sinkEndpoint, connector, stream, handler, withResult) {
        this.sinkEndpoint = sinkEndpoint;
        this.connector = connector;
        this.stream = stream;
        this.handler = handler;
        this.withResult = withResult;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.sinkEndpoint;
    }
    async consume(context, value) {
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const startedSpan = this.#tracer.start(context, "temporal.output", [
                stringAttribute("stream", this.stream.name),
                stringAttribute("endpoint", this.sinkEndpoint.name)
            ]);
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
            const result = await this.connector.submitEndpoint(context, this.sinkEndpoint.id, envelope, this.withResult);
            if (this.withResult) {
                const resultStream = this.stream;
                await resultStream.consumeResult(context, resultStream.serde().deserialize(result.payload));
            }
        }
        catch (error) {
            failure = errorFromUnknown(error);
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
export function makeTemporalSinkEndpointConsumer(stream) {
    return makeTemporalSinkEndpointConsumerWithHandler(stream, new DirectTemporalEndpointHandler());
}
export function makeTemporalSinkEndpointConsumerWithHandler(stream, handler) {
    return makeSinkConsumer(stream, handler, false);
}
export function makeTemporalSinkEndpointConsumerWithResult(stream) {
    return makeTemporalSinkEndpointConsumerWithResultHandler(stream, new DirectTemporalEndpointHandler());
}
export function makeTemporalSinkEndpointConsumerWithResultHandler(stream, handler) {
    return makeSinkConsumer(stream, handler, true);
}
function makeSinkConsumer(stream, handler, withResult) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
    if (endpointConfig === undefined) {
        throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
    }
    const connector = makeTemporalConnector(endpointConfig.idDataConnector, environment);
    const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment);
    if (dataSink.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`Temporal endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new DataSinkEndpoint(dataSink, endpointConfig.id);
    const consumer = new TemporalSinkConsumer(endpoint, connector, stream, handler, withResult);
    connector.registerEndpointSubmission(endpointConfig.id);
    endpoint.addEndpointConsumer(consumer);
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