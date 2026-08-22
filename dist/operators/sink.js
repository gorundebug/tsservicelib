import { ConsumedStream, ServiceStream } from "../runtime/index.js";
import { ErrorStream } from "./error.js";
export class SinkStream extends ServiceStream {
    #endpointId;
    #errorStream;
    #sinkConsumer;
    constructor(config, source) {
        const environment = source.runtimeEnvironment();
        super(config, environment);
        this.#endpointId = config.idEndpoint;
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
    setSinkConsumer(consumer) {
        this.#sinkConsumer = consumer;
    }
    consume(context, value) {
        const consumer = this.#sinkConsumer;
        if (consumer === undefined) {
            return;
        }
        if (!this.tracingEnabled(context)) {
            return consumer.consume(context, value);
        }
        return this.traceCompletion(context, "stream.sink", (spanContext) => consumer.consume(spanContext, value));
    }
    functionImplementation() {
        return undefined;
    }
}
export class SinkStreamWithResult extends ConsumedStream {
    #endpointId;
    #errorStream;
    #sinkConsumer;
    constructor(config, source) {
        const environment = source.runtimeEnvironment();
        super(config, environment, environment.serdeByName(requireResultType(config)));
        this.#endpointId = config.idEndpoint;
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
    setSinkConsumer(consumer) {
        this.#sinkConsumer = consumer;
    }
    consume(context, value) {
        const consumer = this.#sinkConsumer;
        if (consumer === undefined) {
            return;
        }
        if (!this.tracingEnabled(context)) {
            return consumer.consume(context, value);
        }
        return this.traceCompletion(context, "stream.sink", (spanContext) => consumer.consume(spanContext, value));
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
        throw new Error(`sink stream ${config.name} result valueType is missing`);
    }
    return config.valueType;
}
export function makeSinkStream(config, source) {
    return new SinkStream(config, source);
}
export function makeSinkStreamWithResult(config, source) {
    return new SinkStreamWithResult(config, source);
}
//# sourceMappingURL=sink.js.map