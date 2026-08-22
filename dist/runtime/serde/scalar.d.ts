import { ValueSerde } from "./serde.js";
declare abstract class NumberSerde extends ValueSerde<number> {
    protected abstract readonly width: number;
    protected abstract write(output: DataView, value: number): void;
    protected abstract read(input: DataView): number;
    serialize(value: number, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): number;
    protected validate(value: number): void;
}
declare abstract class IntegerSerde extends NumberSerde {
    protected abstract readonly minimum: number;
    protected abstract readonly maximum: number;
    protected validate(value: number): void;
}
export declare class UInt8Serde extends IntegerSerde {
    protected readonly width = 1;
    protected readonly minimum = 0;
    protected readonly maximum = 255;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class UInt16Serde extends IntegerSerde {
    protected readonly width = 2;
    protected readonly minimum = 0;
    protected readonly maximum = 65535;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class UInt32Serde extends IntegerSerde {
    protected readonly width = 4;
    protected readonly minimum = 0;
    protected readonly maximum = 4294967295;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class Int8Serde extends IntegerSerde {
    protected readonly width = 1;
    protected readonly minimum = -128;
    protected readonly maximum = 127;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class Int16Serde extends IntegerSerde {
    protected readonly width = 2;
    protected readonly minimum = -32768;
    protected readonly maximum = 32767;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class Int32Serde extends IntegerSerde {
    protected readonly width = 4;
    protected readonly minimum = -2147483648;
    protected readonly maximum = 2147483647;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
declare abstract class BigIntegerSerde extends ValueSerde<bigint> {
    protected abstract readonly signed: boolean;
    protected abstract readonly minimum: bigint;
    protected abstract readonly maximum: bigint;
    serialize(value: bigint, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): bigint;
}
export declare class UInt64Serde extends BigIntegerSerde {
    protected readonly signed = false;
    protected readonly minimum = 0n;
    protected readonly maximum: bigint;
}
export declare class Int64Serde extends BigIntegerSerde {
    protected readonly signed = true;
    protected readonly minimum: bigint;
    protected readonly maximum: bigint;
}
export declare class UIntSerde extends UInt64Serde {
}
export declare class IntSerde extends Int64Serde {
}
export declare class BoolSerde extends ValueSerde<boolean> {
    serialize(value: boolean, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): boolean;
}
export declare class RuneSerde extends IntegerSerde {
    protected readonly width = 4;
    protected readonly minimum = -2147483648;
    protected readonly maximum = 2147483647;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class Float32Serde extends NumberSerde {
    protected readonly width = 4;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export declare class Float64Serde extends NumberSerde {
    protected readonly width = 8;
    protected write(output: DataView, value: number): void;
    protected read(input: DataView): number;
}
export {};
//# sourceMappingURL=scalar.d.ts.map