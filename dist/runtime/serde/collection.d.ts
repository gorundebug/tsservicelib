import { type Serde, type SerdeLimits, ValueSerde } from "./serde.js";
export declare class FixedSizeArraySerde<T> extends ValueSerde<readonly T[]> {
    #private;
    constructor(elementSerde: Serde<T>, elementSize: number, limits?: SerdeLimits);
    serialize(value: readonly T[], prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): T[];
    isStub(): boolean;
}
export declare class ArraySerde<T> extends ValueSerde<readonly T[]> {
    #private;
    constructor(elementSerde: Serde<T>, limits?: SerdeLimits);
    serialize(value: readonly T[], prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): T[];
    isStub(): boolean;
}
export declare class StringArraySerde extends ValueSerde<readonly string[]> {
    #private;
    constructor(limits?: SerdeLimits);
    serialize(value: readonly string[], prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): string[];
}
export declare class MapSerde<K, V> extends ValueSerde<Map<K, V>> {
    #private;
    constructor(keyArraySerde: Serde<readonly K[]>, valueArraySerde: Serde<readonly V[]>, maxTotalBytes?: number);
    serialize(value: Map<K, V>, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): Map<K, V>;
    isStub(): boolean;
}
export declare class BoolArraySerde extends FixedSizeArraySerde<boolean> {
    constructor(limits?: SerdeLimits);
}
export declare class Int8ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class UInt8ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class Int16ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class UInt16ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class Int32ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class UInt32ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class Int64ArraySerde extends FixedSizeArraySerde<bigint> {
    constructor(limits?: SerdeLimits);
}
export declare class UInt64ArraySerde extends FixedSizeArraySerde<bigint> {
    constructor(limits?: SerdeLimits);
}
export declare class IntArraySerde extends Int64ArraySerde {
}
export declare class UIntArraySerde extends UInt64ArraySerde {
}
export declare class Float32ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
export declare class Float64ArraySerde extends FixedSizeArraySerde<number> {
    constructor(limits?: SerdeLimits);
}
//# sourceMappingURL=collection.d.ts.map