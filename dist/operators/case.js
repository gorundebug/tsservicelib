import { ConsumedStream } from "../runtime/index.js";
export class WhenStream extends ConsumedStream {
    #caseStream;
    #index;
    #valueType;
    constructor(config, caseStream, index) {
        super(config, caseStream.runtimeEnvironment(), caseStream.runtimeEnvironment().serdeByName(config.valueType));
        this.#caseStream = caseStream;
        this.#index = index;
        this.#valueType = config.valueType;
        caseStream.runtimeEnvironment().registerStream(this);
    }
    get name() {
        return super.name.length > 0
            ? super.name
            : `${this.#caseStream.name}CaseLink${String(this.#index)}`;
    }
    valueType() {
        return this.#valueType;
    }
    whenConsumer() {
        return this.consumer() ?? this;
    }
    consume(context, value) {
        return this.emit(context, value);
    }
    consumeCase(context, value) {
        return this.consumeValidated(context, value);
    }
    functionImplementation() {
        return undefined;
    }
    consumeValidated(context, value) {
        const environment = this.runtimeEnvironment();
        environment.assertSerdeValue(this.#valueType, value);
        return this.consume(context, value);
    }
}
export class CaseStream extends ConsumedStream {
    #buildSwitch;
    #whenStreams = [];
    #selector;
    constructor(config, source, buildSwitch) {
        const environment = source.runtimeEnvironment();
        super(config, environment, source.serde());
        this.#buildSwitch = buildSwitch;
        environment.registerStream(this);
        source.setConsumer(this);
        environment.registerRuntimeBuildable(this);
    }
    addStream(config) {
        const stream = new WhenStream(config, this, this.#whenStreams.length);
        this.#whenStreams.push(stream);
        return stream;
    }
    build() {
        this.#selector = this.#buildSwitch.buildSwitch(this, this.#whenStreams);
    }
    consumers() {
        return this.#whenStreams;
    }
    consume(context, value) {
        if (!this.tracingEnabled(context)) {
            return this.consumeCase(context, value);
        }
        return this.traceCompletion(context, "stream.case", (spanContext) => this.consumeCase(spanContext, value));
    }
    functionImplementation() {
        return this.#buildSwitch;
    }
    consumeCase(context, value) {
        if (this.#selector === undefined) {
            throw new Error(`CaseStream ${this.name} is not built`);
        }
        const index = this.#selector(value);
        const branch = this.#whenStreams[index];
        if (branch === undefined) {
            throw new RangeError(`case selector returned branch ${String(index)}, but only ${String(this.#whenStreams.length)} branches exist`);
        }
        return branch.consumeCase(context, value);
    }
}
export function makeCaseStream(config, source, buildSwitch) {
    return new CaseStream(config, source, buildSwitch);
}
export function makeWhenStream(config, caseStream) {
    return caseStream.addStream(config);
}
//# sourceMappingURL=case.js.map