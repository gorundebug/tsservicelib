import { ConsumedStream } from "../runtime/consumed-stream.js";
export class FilterStream extends ConsumedStream {
    #source;
    #function;
    constructor(config, source, function_) {
        super(config, source.runtimeEnvironment(), source.serde());
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
    async consume(context, value) {
        if (!this.tracingEnabled(context)) {
            await this.consumeFiltered(context, value);
            return;
        }
        await this.traceCompletion(context, "stream.filter", async (spanContext) => {
            await this.consumeFiltered(spanContext, value);
        });
    }
    async consumeFiltered(context, value) {
        if (await this.#function.filter(context, this, value)) {
            await this.emit(context, value);
        }
    }
}
export function makeFilterStream(config, source, function_) {
    return new FilterStream(config, source, function_);
}
//# sourceMappingURL=filter.js.map