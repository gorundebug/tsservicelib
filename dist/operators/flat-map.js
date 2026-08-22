import { ConsumedStream } from "../runtime/index.js";
export class FlatMapStream extends ConsumedStream {
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
            return this.#function.flatMap(context, this, value, this);
        }
        return this.traceCompletion(context, "stream.flatmap", (spanContext) => this.#function.flatMap(spanContext, this, value, this));
    }
}
export function makeFlatMapStream(config, source, function_) {
    return new FlatMapStream(config, source, function_);
}
//# sourceMappingURL=flat-map.js.map