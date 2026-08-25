import { ConsumedStream } from "../runtime/consumed-stream.js";
import { ErrorStream } from "./error.js";
import { StreamLink } from "./stream-link.js";
/** Internal feedback edge. Its graph identity is the owning input stream. */
class ResultLink extends StreamLink {
    #input;
    constructor(input) {
        super(input);
        this.#input = input;
    }
    consume(context, value) {
        return this.#input.consumeResult(context, value);
    }
}
export class InputStream extends ConsumedStream {
    #endpointId;
    #errorStream;
    #resultSource;
    #resultConsumer;
    constructor(config, environment, valueSerde, errorSerde) {
        super(config, environment, valueSerde);
        this.#endpointId = config.idEndpoint;
        this.#errorStream = new ErrorStream(config, environment, errorSerde, this);
        environment.registerStream(this);
    }
    endpointId() {
        return this.#endpointId;
    }
    errorStream() {
        return this.#errorStream;
    }
    resultStream() {
        return this.#resultSource;
    }
    setSource(source) {
        if (this.#resultSource !== undefined) {
            throw new Error(`input stream ${this.name} result source is already set`);
        }
        source.setConsumer(new ResultLink(this));
        this.#resultSource = source;
    }
    setResultConsumer(consumer) {
        this.#resultConsumer = consumer;
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.emit(context, value);
        }
        return this.traceCompletion(context, "stream.input", (spanContext) => this.emit(spanContext, value));
    }
    consumeError(context, value) {
        return this.#errorStream.emit(context, value);
    }
    consumeResult(context, value) {
        return this.#resultConsumer?.consume(context, value);
    }
    functionImplementation() {
        return undefined;
    }
}
export class InputKVStream extends InputStream {
}
export function makeInputStream(config, environment) {
    return new InputStream(config, environment, environment.serdeByName(config.valueType), environment.streamErrorSerde(config.id));
}
export function makeInputKVStream(config, environment) {
    const valueSerde = environment.serdeByName(config.valueType);
    if (!valueSerde.isKeyValue()) {
        throw new Error(`input stream ${config.name} valueType ${config.valueType} is not key-value`);
    }
    return new InputKVStream(config, environment, valueSerde, environment.streamErrorSerde(config.id));
}
//# sourceMappingURL=input.js.map