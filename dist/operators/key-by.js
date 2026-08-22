import { ConsumedStream, makeStreamKeyValueSerde } from "../runtime/index.js";
export class KeyByStream extends ConsumedStream {
    #source;
    #function;
    constructor(config, source, function_) {
        const environment = source.runtimeEnvironment();
        super(config, environment, makeStreamKeyValueSerde(environment.serdeByName(config.keyType), environment.serdeByName(config.valueType)));
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
            return this.#function.keyBy(context, this, value, this);
        }
        return this.traceCompletion(context, "stream.keyby", (spanContext) => this.#function.keyBy(spanContext, this, value, this));
    }
}
export function makeKeyByStream(config, source, function_) {
    return new KeyByStream(config, source, function_);
}
//# sourceMappingURL=key-by.js.map