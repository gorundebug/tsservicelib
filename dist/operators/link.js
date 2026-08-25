import { ConsumedStream } from "../runtime/consumed-stream.js";
/** A graph root whose source is connected after the acyclic graph is built. */
export class LinkStream extends ConsumedStream {
    #source;
    constructor(config, environment) {
        super(config, environment, environment.streamValueSerde(config.id));
        environment.registerStream(this);
    }
    source() {
        return this.#source;
    }
    setSource(source) {
        if (this.#source !== undefined) {
            throw new Error(`cycle link stream ${this.name} source is already set`);
        }
        source.setConsumer(this);
        this.#source = source;
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.emit(context, value);
        }
        return this.traceCompletion(context, "stream.link", (spanContext) => this.emit(spanContext, value));
    }
    functionImplementation() {
        return undefined;
    }
}
export function makeLinkStream(config, environment) {
    return new LinkStream(config, environment);
}
//# sourceMappingURL=link.js.map