import { appendBytes, encodeSize, SerdeReader, validateLimits } from "./framing.js";
import { BoolSerde, Float32Serde, Float64Serde, Int8Serde, Int16Serde, Int32Serde, Int64Serde, UInt8Serde, UInt16Serde, UInt32Serde, UInt64Serde } from "./scalar.js";
import { SerdeError, unlimitedSerdeLimits, ValueSerde } from "./serde.js";
export class FixedSizeArraySerde extends ValueSerde {
    #elementSerde;
    #elementSize;
    #limits;
    constructor(elementSerde, elementSize, limits = unlimitedSerdeLimits) {
        super();
        if (!Number.isSafeInteger(elementSize) || elementSize <= 0) {
            throw new RangeError("elementSize must be a positive safe integer");
        }
        validateLimits(limits);
        this.#elementSerde = elementSerde;
        this.#elementSize = elementSize;
        this.#limits = limits;
    }
    serialize(value, prefix) {
        validateArray(value, this.#limits);
        let output = appendBytes(prefix, encodeSize(value.length));
        for (const element of value) {
            const encoded = this.#elementSerde.serialize(element);
            if (encoded.byteLength !== this.#elementSize) {
                throw new SerdeError("fixed-size element serde returned an invalid width", 0);
            }
            output = appendBytes(output, encoded);
        }
        return output;
    }
    deserialize(data) {
        const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
        const count = reader.readSize(this.#limits.maxContainerElements, "array count");
        const result = [];
        for (let index = 0; index < count; index += 1) {
            result.push(this.#elementSerde.deserialize(reader.read(this.#elementSize, "array element")));
        }
        return result;
    }
    isStub() {
        return this.#elementSerde.isStub();
    }
}
export class ArraySerde extends ValueSerde {
    #elementSerde;
    #limits;
    constructor(elementSerde, limits = unlimitedSerdeLimits) {
        super();
        validateLimits(limits);
        this.#elementSerde = elementSerde;
        this.#limits = limits;
    }
    serialize(value, prefix) {
        validateArray(value, this.#limits);
        let output = appendBytes(prefix, encodeSize(value.length));
        for (const element of value) {
            const encoded = this.#elementSerde.serialize(element);
            output = appendBytes(output, encodeSize(encoded.byteLength));
            output = appendBytes(output, encoded);
        }
        return output;
    }
    deserialize(data) {
        const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
        const count = reader.readSize(this.#limits.maxContainerElements, "array count");
        const result = [];
        for (let index = 0; index < count; index += 1) {
            const length = reader.readSize(data.byteLength, "array element length");
            result.push(this.#elementSerde.deserialize(reader.read(length, "array element")));
        }
        return result;
    }
    isStub() {
        return this.#elementSerde.isStub();
    }
}
export class StringArraySerde extends ValueSerde {
    #limits;
    #encoder = new TextEncoder();
    #decoder = new TextDecoder("utf-8", { fatal: true });
    constructor(limits = unlimitedSerdeLimits) {
        super();
        validateLimits(limits);
        this.#limits = limits;
    }
    serialize(value, prefix) {
        validateArray(value, this.#limits);
        let output = appendBytes(prefix, encodeSize(value.length));
        for (const element of value) {
            if (typeof element !== "string") {
                throw new TypeError("StringArraySerde expects strings");
            }
            const encoded = this.#encoder.encode(element);
            if (encoded.byteLength > this.#limits.maxStringBytes) {
                throw new SerdeError("string exceeds configured limit", 0);
            }
            output = appendBytes(output, encodeSize(encoded.byteLength));
            output = appendBytes(output, encoded);
        }
        return output;
    }
    deserialize(data) {
        const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
        const count = reader.readSize(this.#limits.maxContainerElements, "array count");
        const result = [];
        for (let index = 0; index < count; index += 1) {
            const length = reader.readSize(this.#limits.maxStringBytes, "string length");
            const offset = reader.offset;
            const encoded = reader.read(length, "string");
            try {
                result.push(this.#decoder.decode(encoded));
            }
            catch {
                throw new SerdeError("string is not valid UTF-8", offset);
            }
        }
        return result;
    }
}
export class MapSerde extends ValueSerde {
    #keyArraySerde;
    #valueArraySerde;
    #maxTotalBytes;
    constructor(keyArraySerde, valueArraySerde, maxTotalBytes = Number.MAX_SAFE_INTEGER) {
        super();
        if (!Number.isSafeInteger(maxTotalBytes) || maxTotalBytes < 0) {
            throw new RangeError("maxTotalBytes must be a non-negative safe integer");
        }
        this.#keyArraySerde = keyArraySerde;
        this.#valueArraySerde = valueArraySerde;
        this.#maxTotalBytes = maxTotalBytes;
    }
    serialize(value, prefix) {
        if (!(value instanceof Map)) {
            throw new TypeError("MapSerde expects Map");
        }
        const keys = [];
        const values = [];
        for (const [key, item] of value) {
            keys.push(key);
            values.push(item);
        }
        const encodedKeys = this.#keyArraySerde.serialize(keys);
        const encodedValues = this.#valueArraySerde.serialize(values);
        let output = appendBytes(prefix, encodeSize(encodedKeys.byteLength));
        output = appendBytes(output, encodedKeys);
        output = appendBytes(output, encodeSize(encodedValues.byteLength));
        return appendBytes(output, encodedValues);
    }
    deserialize(data) {
        const reader = new SerdeReader(data, this.#maxTotalBytes);
        const keysLength = reader.readSize(data.byteLength, "map keys length");
        const keys = this.#keyArraySerde.deserialize(reader.read(keysLength, "map keys"));
        const valuesLength = reader.readSize(data.byteLength, "map values length");
        const values = this.#valueArraySerde.deserialize(reader.read(valuesLength, "map values"));
        if (keys.length !== values.length) {
            throw new SerdeError("map key and value counts do not match", reader.offset);
        }
        const result = new Map();
        const valueIterator = values.values();
        for (const key of keys) {
            const next = valueIterator.next();
            if (next.done) {
                throw new SerdeError("map value is unexpectedly absent", reader.offset);
            }
            result.set(key, next.value);
        }
        return result;
    }
    isStub() {
        return this.#keyArraySerde.isStub() || this.#valueArraySerde.isStub();
    }
}
export class BoolArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new BoolSerde(), 1, limits);
    }
}
export class Int8ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new Int8Serde(), 1, limits);
    }
}
export class UInt8ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new UInt8Serde(), 1, limits);
    }
}
export class Int16ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new Int16Serde(), 2, limits);
    }
}
export class UInt16ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new UInt16Serde(), 2, limits);
    }
}
export class Int32ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new Int32Serde(), 4, limits);
    }
}
export class UInt32ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new UInt32Serde(), 4, limits);
    }
}
export class Int64ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new Int64Serde(), 8, limits);
    }
}
export class UInt64ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new UInt64Serde(), 8, limits);
    }
}
export class IntArraySerde extends Int64ArraySerde {
}
export class UIntArraySerde extends UInt64ArraySerde {
}
export class Float32ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new Float32Serde(), 4, limits);
    }
}
export class Float64ArraySerde extends FixedSizeArraySerde {
    constructor(limits) {
        super(new Float64Serde(), 8, limits);
    }
}
function validateArray(value, limits) {
    if (!Array.isArray(value)) {
        throw new TypeError("array serde expects an array");
    }
    if (value.length > limits.maxContainerElements) {
        throw new SerdeError("array exceeds configured element limit", 0);
    }
}
//# sourceMappingURL=collection.js.map