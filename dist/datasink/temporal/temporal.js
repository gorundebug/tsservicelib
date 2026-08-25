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
class TemporalSinkConsumer {
    sinkEndpoint;
    connector;
    stream;
    withResult;
    #tracer;
    constructor(sinkEndpoint, connector, stream, withResult) {
        this.sinkEndpoint = sinkEndpoint;
        this.connector = connector;
        this.stream = stream;
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
        try {
            const executionId = context.streamId() ?? newStreamId();
            const remainingMs = context.remainingMs();
            const envelope = {
                version: 1,
                endpointId: this.sinkEndpoint.id,
                executionId,
                streamId: context.streamId() ?? executionId,
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
            this.sinkEndpoint.onRequestEnd(context, started, failure);
            span?.end();
        }
    }
}
export function makeTemporalSinkEndpointConsumer(stream) {
    return makeSinkConsumer(stream, false);
}
export function makeTemporalSinkEndpointConsumerWithResult(stream) {
    return makeSinkConsumer(stream, true);
}
function makeSinkConsumer(stream, withResult) {
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
    const consumer = new TemporalSinkConsumer(endpoint, connector, stream, withResult);
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