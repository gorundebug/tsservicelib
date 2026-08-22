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

abstract class NumberSerde extends ValueSerde<number> {
  protected abstract readonly width: number;
  protected abstract write(output: DataView, value: number): void;
  protected abstract read(input: DataView): number;

  public serialize(value: number, prefix?: Uint8Array): Uint8Array {
    this.validate(value);
    const output = new Uint8Array(this.width);
    this.write(view(output), value);
    return appendBytes(prefix, output);
  }

  public deserialize(data: Uint8Array): number {
    const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
    return this.read(view(reader.read(this.width, this.constructor.name)));
  }

  protected validate(value: number): void {
    if (typeof value !== "number") {
      throw new TypeError(`${this.constructor.name} expects a number`);
    }
  }
}

abstract class IntegerSerde extends NumberSerde {
  protected abstract readonly minimum: number;
  protected abstract readonly maximum: number;

  protected override validate(value: number): void {
    super.validate(value);
    if (!Number.isInteger(value) || value < this.minimum || value > this.maximum) {
      throw new RangeError(
        `${this.constructor.name} expects an integer in [${String(this.minimum)}, ${String(this.maximum)}]`
      );
    }
  }
}

export class UInt8Serde extends IntegerSerde {
  protected readonly width = 1;
  protected readonly minimum = 0;
  protected readonly maximum = maxUInt8;
  protected write(output: DataView, value: number): void {
    output.setUint8(0, value);
  }
  protected read(input: DataView): number {
    return input.getUint8(0);
  }
}

export class UInt16Serde extends IntegerSerde {
  protected readonly width = 2;
  protected readonly minimum = 0;
  protected readonly maximum = maxUInt16;
  protected write(output: DataView, value: number): void {
    output.setUint16(0, value, false);
  }
  protected read(input: DataView): number {
    return input.getUint16(0, false);
  }
}

export class UInt32Serde extends IntegerSerde {
  protected readonly width = 4;
  protected readonly minimum = 0;
  protected readonly maximum = maxUInt32;
  protected write(output: DataView, value: number): void {
    output.setUint32(0, value, false);
  }
  protected read(input: DataView): number {
    return input.getUint32(0, false);
  }
}

export class Int8Serde extends IntegerSerde {
  protected readonly width = 1;
  protected readonly minimum = minInt8;
  protected readonly maximum = maxInt8;
  protected write(output: DataView, value: number): void {
    output.setInt8(0, value);
  }
  protected read(input: DataView): number {
    return input.getInt8(0);
  }
}

export class Int16Serde extends IntegerSerde {
  protected readonly width = 2;
  protected readonly minimum = minInt16;
  protected readonly maximum = maxInt16;
  protected write(output: DataView, value: number): void {
    output.setUint16(0, value ^ 0x8000, false);
  }
  protected read(input: DataView): number {
    return ((input.getUint16(0, false) ^ 0x8000) << 16) >> 16;
  }
}

export class Int32Serde extends IntegerSerde {
  protected readonly width = 4;
  protected readonly minimum = minInt32;
  protected readonly maximum = maxInt32;
  protected write(output: DataView, value: number): void {
    output.setUint32(0, (value ^ 0x80000000) >>> 0, false);
  }
  protected read(input: DataView): number {
    return (input.getUint32(0, false) ^ 0x80000000) | 0;
  }
}

abstract class BigIntegerSerde extends ValueSerde<bigint> {
  protected abstract readonly signed: boolean;
  protected abstract readonly minimum: bigint;
  protected abstract readonly maximum: bigint;

  public serialize(value: bigint, prefix?: Uint8Array): Uint8Array {
    if (typeof value !== "bigint" || value < this.minimum || value > this.maximum) {
      throw new RangeError(
        `${this.constructor.name} expects a bigint in [${this.minimum.toString()}, ${this.maximum.toString()}]`
      );
    }
    const output = new Uint8Array(8);
    const encoded = this.signed ? BigInt.asUintN(64, value) ^ (1n << 63n) : value;
    view(output).setBigUint64(0, encoded, false);
    return appendBytes(prefix, output);
  }

  public deserialize(data: Uint8Array): bigint {
    const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
    const encoded = view(reader.read(8, this.constructor.name)).getBigUint64(0, false);
    return this.signed ? BigInt.asIntN(64, encoded ^ (1n << 63n)) : encoded;
  }
}

export class UInt64Serde extends BigIntegerSerde {
  protected readonly signed = false;
  protected readonly minimum = 0n;
  protected readonly maximum = maxUInt64;
}

export class Int64Serde extends BigIntegerSerde {
  protected readonly signed = true;
  protected readonly minimum = minInt64;
  protected readonly maximum = maxInt64;
}

export class UIntSerde extends UInt64Serde {}
export class IntSerde extends Int64Serde {}

export class BoolSerde extends ValueSerde<boolean> {
  public serialize(value: boolean, prefix?: Uint8Array): Uint8Array {
    if (typeof value !== "boolean") {
      throw new TypeError("BoolSerde expects a boolean");
    }
    return appendBytes(prefix, Uint8Array.of(value ? 1 : 0));
  }

  public deserialize(data: Uint8Array): boolean {
    const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
    return reader.read(1, "BoolSerde")[0] !== 0;
  }
}

export class RuneSerde extends IntegerSerde {
  protected readonly width = 4;
  protected readonly minimum = minInt32;
  protected readonly maximum = maxInt32;
  protected write(output: DataView, value: number): void {
    output.setUint32(0, value >>> 0, false);
  }
  protected read(input: DataView): number {
    return input.getInt32(0, false);
  }
}

export class Float32Serde extends NumberSerde {
  protected readonly width = 4;
  protected write(output: DataView, value: number): void {
    output.setFloat32(0, value, false);
  }
  protected read(input: DataView): number {
    return input.getFloat32(0, false);
  }
}

export class Float64Serde extends NumberSerde {
  protected readonly width = 8;
  protected write(output: DataView, value: number): void {
    output.setFloat64(0, value, false);
  }
  protected read(input: DataView): number {
    return input.getFloat64(0, false);
  }
}
