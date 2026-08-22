import { type SerdeLimits, ValueSerde } from "./serde.js";
import type { SerdeType } from "./registry.js";
export declare class JsonSerde<T> extends ValueSerde<T> {
    #private;
    private readonly type;
    constructor(type: SerdeType<T>, limits?: SerdeLimits);
    serialize(value: T, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): T;
}
//# sourceMappingURL=json.d.ts.map