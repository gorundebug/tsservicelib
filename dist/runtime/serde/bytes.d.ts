import { type SerdeLimits, ValueSerde } from "./serde.js";
export declare class BytesSerde extends ValueSerde<Uint8Array> {
    #private;
    constructor(limits?: SerdeLimits);
    serialize(value: Uint8Array, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): Uint8Array;
}
export declare class StringSerde extends ValueSerde<string> {
    #private;
    constructor(limits?: SerdeLimits);
    serialize(value: string, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): string;
}
//# sourceMappingURL=bytes.d.ts.map