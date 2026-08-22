import { appendBytes, encodeSize, SerdeReader, validateLimits } from "./framing.js";
import { SerdeError, unlimitedSerdeLimits, ValueSerde } from "./serde.js";
export class BytesSerde extends ValueSerde {
    #limits;
    constructor(limits = unlimitedSerdeLimits) {
        super();
        validateLimits(limits);
        this.#limits = limits;
    }
    serialize(value, prefix) {
        if (!(value instanceof Uint8Array)) {
            throw new TypeError("BytesSerde expects Uint8Array");
        }
        if (value.byteLength > this.#limits.maxBytes) {
            throw new SerdeError("bytes exceed configured limit", 0);
        }
        return appendBytes(prefix, appendBytes(encodeSize(value.byteLength), value));
    }
    deserialize(data) {
        const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
        const length = reader.readSize(this.#limits.maxBytes, "bytes length");
        return reader.read(length, "bytes");
    }
}
export class StringSerde extends ValueSerde {
    #limits;
    #encoder = new TextEncoder();
    #decoder = new TextDecoder("utf-8", { fatal: true });
    constructor(limits = unlimitedSerdeLimits) {
        super();
        validateLimits(limits);
        this.#limits = limits;
    }
    serialize(value, prefix) {
        if (typeof value !== "string") {
            throw new TypeError("StringSerde expects a string");
        }
        const encoded = this.#encoder.encode(value);
        if (encoded.byteLength > this.#limits.maxStringBytes) {
            throw new SerdeError("string exceeds configured limit", 0);
        }
        return appendBytes(prefix, appendBytes(encodeSize(encoded.byteLength), encoded));
    }
    deserialize(data) {
        const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
        const lengthOffset = reader.offset;
        const length = reader.readSize(this.#limits.maxStringBytes, "string length");
        const payload = reader.read(length, "string");
        try {
            return this.#decoder.decode(payload);
        }
        catch (error) {
            throw new SerdeError(`invalid UTF-8 string${error instanceof Error ? `: ${error.message}` : ""}`, lengthOffset + 8);
        }
    }
}
//# sourceMappingURL=bytes.js.map