import { makeCollector } from "./collector.js";
import { ServiceStream } from "./stream.js";
export class ConsumedStream extends ServiceStream {
    #downstream;
    #consumer;
    #serde;
    constructor(config, environment, serde) {
        super(config, environment);
        this.#serde = serde;
    }
    serde() {
        return this.#serde;
    }
    typeName() {
        return this.#serde.typeName();
    }
    consumer() {
        return this.#consumer;
    }
    consumers() {
        return this.#consumer === undefined ? [] : [this.#consumer];
    }
    setConsumer(consumer) {
        if (this.#consumer !== undefined) {
            throw new Error(`consumer already assigned to stream ${this.name}`);
        }
        this.#consumer = consumer;
        this.#downstream = this.runtimeEnvironment().makeCaller(this, consumer);
    }
    emit(context, value) {
        return this.#downstream?.consume(context, value);
    }
    out(context, value) {
        return this.emit(context, value);
    }
    collector() {
        return makeCollector(this.#downstream);
    }
}
//# sourceMappingURL=consumed-stream.js.map