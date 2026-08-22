import { ConsumedStream } from "../runtime/index.js";
import { StreamLink } from "./stream-link.js";
class MergeLink extends StreamLink {
    #merge;
    constructor(merge) {
        super(merge);
        this.#merge = merge;
    }
    consume(context, value) {
        return this.#merge.consume(context, value);
    }
}
export class MergeStream extends ConsumedStream {
    constructor(config, sources) {
        const environment = sources[0].runtimeEnvironment();
        for (const source of sources) {
            if (source.runtimeEnvironment() !== environment) {
                throw new Error(`merge stream ${config.name} sources belong to different environments`);
            }
        }
        super(config, environment, sources[0].serde());
        environment.registerStream(this);
        for (const source of sources) {
            source.setConsumer(new MergeLink(this));
        }
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.emit(context, value);
        }
        return this.traceCompletion(context, "stream.merge", (spanContext) => this.emit(spanContext, value));
    }
    functionImplementation() {
        return undefined;
    }
}
export function makeMergeStream(config, source, ...sources) {
    return new MergeStream(config, [source, ...sources]);
}
//# sourceMappingURL=merge.js.map