import { ConsumedStream } from "../runtime/consumed-stream.js";
class SplitLink {
    #split;
    #index;
    #consumer;
    #caller;
    constructor(split, index) {
        this.#split = split;
        this.#index = index;
    }
    get id() {
        return this.#split.id;
    }
    get name() {
        return `${this.#split.name}SplitLink${String(this.#index)}`;
    }
    get transformationName() {
        return this.#split.transformationName;
    }
    runtimeEnvironment() {
        return this.#split.runtimeEnvironment();
    }
    config() {
        return this.#split.config();
    }
    serde() {
        return this.#split.serde();
    }
    typeName() {
        return this.#split.typeName();
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
        this.#caller = this.runtimeEnvironment().makeCaller(this, consumer);
    }
    isAsync() {
        return this.#caller?.isAsync() ?? false;
    }
    emit(context, value) {
        return this.#caller?.consume(context, value);
    }
}
export class SplitStream extends ConsumedStream {
    #links = [];
    #dispatchOrder = [];
    constructor(config, source) {
        const environment = source.runtimeEnvironment();
        super(config, environment, source.serde());
        source.setConsumer(this);
        environment.registerStream(this);
        environment.registerRuntimeBuildable(this);
    }
    addStream() {
        const link = new SplitLink(this, this.#links.length);
        this.#links.push(link);
        return link;
    }
    build() {
        for (const [index, link] of this.#links.entries()) {
            if (link.consumer() === undefined) {
                throw new Error(`link with index ${String(index)} for SplitStream ${this.name} does not have consumer`);
            }
        }
        this.#dispatchOrder = [...this.#links].sort((left, right) => Number(right.isAsync()) - Number(left.isAsync()));
    }
    consumers() {
        return this.#links.map((link, index) => {
            const consumer = link.consumer();
            if (consumer === undefined) {
                throw new Error(`link with index ${String(index)} for SplitStream ${this.name} does not have consumer`);
            }
            return consumer;
        });
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.emitLinks(context, value);
        }
        return this.traceCompletion(context, "stream.split", (spanContext) => this.emitLinks(spanContext, value));
    }
    emitLinks(context, value) {
        let pending;
        for (const link of this.#dispatchOrder) {
            if (pending === undefined) {
                const completion = link.emit(context, value);
                if (completion !== undefined)
                    pending = completion;
            }
            else {
                pending = pending.then(() => link.emit(context, value));
            }
        }
        return pending;
    }
    functionImplementation() {
        return undefined;
    }
}
export function makeSplitStream(config, source) {
    return new SplitStream(config, source);
}
//# sourceMappingURL=split.js.map