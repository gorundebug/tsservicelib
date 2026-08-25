import { ConsumedStream } from "../runtime/consumed-stream.js";
export class MapStream extends ConsumedStream {
    #source;
    #function;
    constructor(config, source, function_) {
        const environment = source.runtimeEnvironment();
        super(config, environment, environment.serdeByName(config.valueType));
        this.#source = source;
        this.#function = function_;
        source.setConsumer(this);
        this.runtimeEnvironment().registerStream(this);
    }
    source() {
        return this.#source;
    }
    functionImplementation() {
        return this.#function;
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.#function.map(context, this, value, this);
        }
        return this.traceCompletion(context, "stream.map", (spanContext) => this.#function.map(spanContext, this, value, this));
    }
}
export function makeMapStream(config, source, function_) {
    return new MapStream(config, source, function_);
}
//# sourceMappingURL=map.js.map