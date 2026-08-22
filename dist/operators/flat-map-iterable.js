import { ConsumedStream } from "../runtime/index.js";
const utf8Encoder = new TextEncoder();
function stringItems(value, valueType) {
    if (valueType === "int32") {
        return (function* codePoints() {
            for (const character of value) {
                const codePoint = character.codePointAt(0);
                if (codePoint === undefined) {
                    throw new Error("string iteration produced an empty character");
                }
                yield codePoint;
            }
        })();
    }
    if (valueType === "uint8") {
        return utf8Encoder.encode(value);
    }
    throw new TypeError(`FlatMapIterable string output type must be int32 or uint8, got ${valueType}`);
}
function isIndexedIterable(value) {
    if (typeof value !== "object" ||
        value === null ||
        !(Symbol.iterator in value) ||
        !("length" in value)) {
        return false;
    }
    const length = value.length;
    return typeof length === "number" && Number.isSafeInteger(length) && length >= 0;
}
export class FlatMapIterableStream extends ConsumedStream {
    #source;
    #valueType;
    constructor(config, source) {
        super(config, source.runtimeEnvironment(), source.runtimeEnvironment().serdeByName(config.valueType));
        this.#source = source;
        this.#valueType = config.valueType;
        source.setConsumer(this);
        this.runtimeEnvironment().registerStream(this);
    }
    source() {
        return this.#source;
    }
    functionImplementation() {
        return undefined;
    }
    async consume(context, value) {
        if (!this.tracingEnabled(context)) {
            await this.emitItems(context, value);
            return;
        }
        await this.traceCompletion(context, "stream.flatmapiterable", async (spanContext) => {
            await this.emitItems(spanContext, value);
        });
    }
    async emitItems(context, value) {
        if (typeof value === "string") {
            for (const item of stringItems(value, this.#valueType)) {
                const output = item;
                const environment = this.runtimeEnvironment();
                environment.assertSerdeValue(this.#valueType, output);
                await this.emit(context, output);
            }
            return;
        }
        if (!isIndexedIterable(value)) {
            throw new TypeError(`FlatMapIterable stream ${this.name} requires an array or typed array`);
        }
        await this.emitIndexed(context, value);
    }
    async emitIndexed(context, value) {
        for (const item of value) {
            await this.emit(context, item);
        }
    }
}
export function makeFlatMapIterableStream(config, source) {
    return new FlatMapIterableStream(config, source);
}
//# sourceMappingURL=flat-map-iterable.js.map