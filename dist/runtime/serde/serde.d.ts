export declare class SerdeError extends Error {
    readonly offset: number;
    constructor(message: string, offset: number);
}
export interface SerdeLimits {
    readonly maxStringBytes: number;
    readonly maxBytes: number;
    readonly maxContainerElements: number;
    readonly maxTotalBytes: number;
}
export declare const unlimitedSerdeLimits: Readonly<SerdeLimits>;
export interface Serde<T> {
    serialize(value: T, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): T;
    isStub(): boolean;
}
export interface StreamSerde<T> extends Serde<T> {
    typeName(): string;
    isKeyValue(): boolean;
    serializeKey(value: T): Uint8Array | undefined;
    serializeValue(value: T): Uint8Array;
    deserializeKeyValue(key: Uint8Array | undefined, value: Uint8Array): T;
}
export declare abstract class ValueSerde<T> implements Serde<T> {
    abstract serialize(value: T, prefix?: Uint8Array): Uint8Array;
    abstract deserialize(data: Uint8Array): T;
    isStub(): boolean;
}
export declare class StubSerde<T> implements Serde<T> {
    serialize(value: T, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): T;
    isStub(): boolean;
}
//# sourceMappingURL=serde.d.ts.map