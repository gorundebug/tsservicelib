import { appendBytes, SerdeReader, view } from "./framing.js";
import { ValueSerde } from "./serde.js";
const minInt8 = -0x80;
const maxInt8 = 0x7f;
const minInt16 = -0x8000;
const maxInt16 = 0x7fff;
const minInt32 = -0x80000000;
const maxInt32 = 0x7fffffff;
const minInt64 = -(1n << 63n);
const maxInt64 = (1n << 63n) - 1n;
const maxUInt8 = 0xff;
const maxUInt16 = 0xffff;
const maxUInt32 = 0xffffffff;
const maxUInt64 = (1n << 64n) - 1n;
class NumberSerde extends ValueSerde {
    serialize(value, prefix) {
        this.validate(value);
        const output = new Uint8Array(this.width);
        this.write(view(output), value);
        return appendBytes(prefix, output);
    }
    deserialize(data) {
        const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
        return this.read(view(reader.read(this.width, this.constructor.name)));
    }
    validate(value) {
        if (typeof value !== "number") {
            throw new TypeError(`${this.constructor.name} expects a number`);
        }
    }
}
class IntegerSerde extends NumberSerde {
    validate(value) {
        super.validate(value);
        if (!Number.isInteger(value) || value < this.minimum || value > this.maximum) {
            throw new RangeError(`${this.constructor.name} expects an integer in [${String(this.minimum)}, ${String(this.maximum)}]`);
        }
    }
}
export class UInt8Serde extends IntegerSerde {
    width = 1;
    minimum = 0;
    maximum = maxUInt8;
    write(output, value) {
        output.setUint8(0, value);
    }
    read(input) {
        return input.getUint8(0);
    }
}
export class UInt16Serde extends IntegerSerde {
    width = 2;
    minimum = 0;
    maximum = maxUInt16;
    write(output, value) {
        output.setUint16(0, value, false);
    }
    read(input) {
        return input.getUint16(0, false);
    }
}
export class UInt32Serde extends IntegerSerde {
    width = 4;
    minimum = 0;
    maximum = maxUInt32;
    write(output, value) {
        output.setUint32(0, value, false);
    }
    read(input) {
        return input.getUint32(0, false);
    }
}
export class Int8Serde extends IntegerSerde {
    width = 1;
    minimum = minInt8;
    maximum = maxInt8;
    write(output, value) {
        output.setInt8(0, value);
    }
    read(input) {
        return input.getInt8(0);
    }
}
export class Int16Serde extends IntegerSerde {
    width = 2;
    minimum = minInt16;
    maximum = maxInt16;
    write(output, value) {
        output.setUint16(0, value ^ 0x8000, false);
    }
    read(input) {
        return ((input.getUint16(0, false) ^ 0x8000) << 16) >> 16;
    }
}
export class Int32Serde extends IntegerSerde {
    width = 4;
    minimum = minInt32;
    maximum = maxInt32;
    write(output, value) {
        output.setUint32(0, (value ^ 0x80000000) >>> 0, false);
    }
    read(input) {
        return (input.getUint32(0, false) ^ 0x80000000) | 0;
    }
}
class BigIntegerSerde extends ValueSerde {
    serialize(value, prefix) {
        if (typeof value !== "bigint" || value < this.minimum || value > this.maximum) {
            throw new RangeError(`${this.constructor.name} expects a bigint in [${this.minimum.toString()}, ${this.maximum.toString()}]`);
        }
        const output = new Uint8Array(8);
        const encoded = this.signed ? BigInt.asUintN(64, value) ^ (1n << 63n) : value;
        view(output).setBigUint64(0, encoded, false);
        return appendBytes(prefix, output);
    }
    deserialize(data) {
        const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
        const encoded = view(reader.read(8, this.constructor.name)).getBigUint64(0, false);
        return this.signed ? BigInt.asIntN(64, encoded ^ (1n << 63n)) : encoded;
    }
}
export class UInt64Serde extends BigIntegerSerde {
    signed = false;
    minimum = 0n;
    maximum = maxUInt64;
}
export class Int64Serde extends BigIntegerSerde {
    signed = true;
    minimum = minInt64;
    maximum = maxInt64;
}
export class UIntSerde extends UInt64Serde {
}
export class IntSerde extends Int64Serde {
}
export class BoolSerde extends ValueSerde {
    serialize(value, prefix) {
        if (typeof value !== "boolean") {
            throw new TypeError("BoolSerde expects a boolean");
        }
        return appendBytes(prefix, Uint8Array.of(value ? 1 : 0));
    }
    deserialize(data) {
        const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
        return reader.read(1, "BoolSerde")[0] !== 0;
    }
}
export class RuneSerde extends IntegerSerde {
    width = 4;
    minimum = minInt32;
    maximum = maxInt32;
    write(output, value) {
        output.setUint32(0, value >>> 0, false);
    }
    read(input) {
        return input.getInt32(0, false);
    }
}
export class Float32Serde extends NumberSerde {
    width = 4;
    write(output, value) {
        output.setFloat32(0, value, false);
    }
    read(input) {
        return input.getFloat32(0, false);
    }
}
export class Float64Serde extends NumberSerde {
    width = 8;
    write(output, value) {
        output.setFloat64(0, value, false);
    }
    read(input) {
        return input.getFloat64(0, false);
    }
}
//# sourceMappingURL=scalar.js.map