import { ConsumedStream } from "../runtime/consumed-stream.js";
import {} from "../runtime/stream.js";
import { ErrorStream } from "./error.js";
export class QueryStream extends ConsumedStream {
    #endpointId;
    #errorStream;
    #inputSerde;
    #queryConsumer;
    constructor(config, source) {
        const environment = source.runtimeEnvironment();
        super(config, environment, environment.serdeByName(requireResultType(config)));
        this.#endpointId = config.idEndpoint;
        this.#inputSerde = source.serde();
        this.#errorStream = new ErrorStream(config, environment, environment.streamErrorSerde(config.id), this);
        source.setConsumer(this);
        environment.registerStream(this);
    }
    endpointId() {
        return this.#endpointId;
    }
    errorStream() {
        return this.#errorStream;
    }
    setQueryConsumer(consumer) {
        this.#queryConsumer = consumer;
    }
    inputSerde() {
        return this.#inputSerde;
    }
    consume(context, value) {
        const endpoint = this.runtimeEnvironment().runtimeConfig().endpointById(this.#endpointId);
        if (endpoint !== undefined && "enabled" in endpoint && endpoint.enabled === false)
            return;
        const consumer = this.#queryConsumer;
        if (consumer === undefined) {
            return;
        }
        if (!this.tracingEnabled(context)) {
            return consumer.consume(context, value);
        }
        return this.traceCompletion(context, "stream.query", (spanContext) => consumer.consume(spanContext, value));
    }
    consumeResult(context, value) {
        return this.emit(context, value);
    }
    functionImplementation() {
        return undefined;
    }
}
function requireResultType(config) {
    if (config.valueType === undefined) {
        throw new Error(`query stream ${config.name} result valueType is missing`);
    }
    return config.valueType;
}
export function makeQueryStream(config, source) {
    return new QueryStream(config, source);
}
//# sourceMappingURL=query.js.map