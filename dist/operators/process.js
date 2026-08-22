import { ConsumedStream } from "../runtime/index.js";
import { ErrorStream } from "./error.js";
export class ProcessStream extends ConsumedStream {
    #source;
    #function;
    #errorStream;
    constructor(config, source, function_) {
        const environment = source.runtimeEnvironment();
        super(config, environment, environment.streamValueSerde(config.id));
        this.#source = source;
        this.#function = function_;
        this.#errorStream = new ErrorStream(config, environment, environment.streamErrorSerde(config.id), this);
        source.setConsumer(this);
        this.runtimeEnvironment().registerStream(this);
    }
    source() {
        return this.#source;
    }
    errorStream() {
        return this.#errorStream;
    }
    functionImplementation() {
        return this.#function;
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.#function.process(context, this, value, this, this.#errorStream);
        }
        return this.traceCompletion(context, "stream.process", (spanContext) => this.#function.process(spanContext, this, value, this, this.#errorStream));
    }
}
export function makeProcessStream(config, source, function_) {
    return new ProcessStream(config, source, function_);
}
//# sourceMappingURL=process.js.map