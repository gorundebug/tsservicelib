import { makeEndpointTraceAttributes } from "../../runtime/endpoint-tracing.js";
import { DataConnectorType, DataSinkEndpoint, DataSinkEndpointConsumer, FunctionCollector, OutputDataSink, Context, errorFromUnknown, spanError, stringAttribute } from "../../runtime/index.js";
class CustomSinkEndpoint extends DataSinkEndpoint {
    #bindings = [];
    bind(binding) {
        this.#bindings.push(binding);
        this.addEndpointConsumer(binding);
    }
    async start(context) {
        for (const binding of this.#bindings)
            await binding.start(context);
    }
    async stop(context) {
        await Promise.all(this.#bindings.map(async (binding) => binding.stop(context)));
    }
}
export class CustomDataSink extends OutputDataSink {
    #started = false;
    constructor(connectorId, environment) {
        super(connectorId, environment);
        if (this.config().type !== DataConnectorType.Custom) {
            throw new Error(`data sink ${this.name} is not custom`);
        }
    }
    async start(context) {
        if (this.#started)
            throw new Error(`custom data sink ${this.name} is already started`);
        this.#started = true;
        try {
            for (const endpoint of this.customEndpoints())
                await endpoint.start(context);
        }
        catch (error) {
            this.#started = false;
            await Promise.allSettled(this.customEndpoints().map(async (endpoint) => endpoint.stop(Context.background())));
            throw error;
        }
    }
    async stop(context) {
        if (!this.#started)
            return;
        this.#started = false;
        await Promise.all(this.customEndpoints().map(async (endpoint) => endpoint.stop(context)));
    }
    customEndpoints() {
        return this.endpoints().map((endpoint) => {
            if (!(endpoint instanceof CustomSinkEndpoint)) {
                throw new Error(`sink endpoint ${endpoint.name} is not custom`);
            }
            return endpoint;
        });
    }
}
class CustomEndpointConsumer {
    #base;
    #stream;
    #handler;
    #resultStream;
    #traceAttributes;
    #tracer;
    #sinkCallback;
    constructor(endpoint, stream, handler) {
        this.#base = new DataSinkEndpointConsumer(endpoint, stream);
        this.#stream = stream;
        this.#handler = handler;
        this.#resultStream = new FunctionCollector((context, value) => stream.errorStream().consume(context, value));
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
        this.#traceAttributes = makeEndpointTraceAttributes(stream, endpoint.name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    setSinkCallback(callback) {
        this.#sinkCallback = callback;
    }
    start(_context) {
        void _context;
        return Promise.resolve();
    }
    stop(_context) {
        void _context;
        return Promise.resolve();
    }
    async consume(context, value) {
        const endpoint = this.#base.endpoint();
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "local.output", this.#traceAttributes);
            context = started.context;
            span = started.span;
        }
        try {
            await this.consumeTraced(context, value, endpoint, span);
        }
        finally {
            span?.end();
        }
    }
    async consumeTraced(context, value, endpoint, span) {
        const originalContext = context;
        const streamId = this.#handler.getStreamId(context, value);
        context = context.withStreamId(streamId);
        span?.setAttributes([stringAttribute("stream_id", streamId)]);
        let handlerContext;
        let handlerState;
        try {
            const started = await this.#handler.beginRequest(context, this.#stream);
            handlerContext = started.context;
            handlerState = started.state;
            span?.addEvent("begin_request");
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            if (span !== undefined)
                spanError(span, failure);
            endpoint.onBeginRequestFailed(context, failure);
            return;
        }
        const requestStarted = endpoint.onRequestStart(handlerContext);
        let failure;
        try {
            await this.#handler.consumeMessage(handlerContext, this.#stream, handlerState, value, this.#resultStream);
            span?.addEvent("consume_message");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            if (span !== undefined)
                spanError(span, failure);
            span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
        }
        finally {
            try {
                await this.#handler.endRequest(handlerContext, this.#stream, failure, handlerState);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                if (span !== undefined)
                    spanError(span, failure);
            }
            finally {
                endpoint.onRequestEnd(handlerContext, requestStarted, failure);
            }
        }
        await this.#sinkCallback?.done(originalContext, value, failure);
    }
}
export function makeCustomEndpointConsumer(stream, handler) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
    if (endpointConfig === undefined) {
        throw new Error(`endpoint config ${String(stream.endpointId())} not found`);
    }
    const connectorConfig = environment
        .runtimeConfig()
        .dataConnectorById(endpointConfig.idDataConnector);
    if (connectorConfig === undefined) {
        throw new Error(`data connector config ${String(endpointConfig.idDataConnector)} not found`);
    }
    if (connectorConfig.type !== DataConnectorType.Custom) {
        throw new Error(`data connector ${connectorConfig.name} is not custom`);
    }
    const existing = environment.dataSinkById(connectorConfig.id);
    const dataSink = existing ?? new CustomDataSink(connectorConfig.id, environment);
    if (!(dataSink instanceof CustomDataSink)) {
        throw new Error(`data sink ${connectorConfig.name} is not custom`);
    }
    if (existing === undefined)
        environment.addDataSink(dataSink);
    const existingEndpoint = dataSink.endpoint(endpointConfig.id);
    if (existingEndpoint !== undefined && !(existingEndpoint instanceof CustomSinkEndpoint)) {
        throw new Error(`endpoint ${endpointConfig.name} is not a custom sink endpoint`);
    }
    const endpoint = existingEndpoint === undefined
        ? new CustomSinkEndpoint(dataSink, endpointConfig.id)
        : existingEndpoint;
    const consumer = new CustomEndpointConsumer(endpoint, stream, handler);
    endpoint.bind(consumer);
    if (existingEndpoint === undefined)
        dataSink.addEndpoint(endpoint);
    stream.setSinkConsumer(consumer);
    return consumer;
}
//# sourceMappingURL=custom.js.map