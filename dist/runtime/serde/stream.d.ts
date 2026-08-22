import type { KeyValue } from "../datastruct/index.js";
import { type Serde, type StreamSerde } from "./serde.js";
export declare class StreamKeyValueSerde<K, V> implements StreamSerde<KeyValue<K, V>> {
    private readonly keySerde;
    private readonly valueSerde;
    constructor(keySerde: Serde<K>, valueSerde: Serde<V>);
    serialize(value: KeyValue<K, V>, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): KeyValue<K, V>;
    isStub(): boolean;
    typeName(): string;
    isKeyValue(): boolean;
    serializeKey(value: KeyValue<K, V>): Uint8Array;
    serializeValue(value: KeyValue<K, V>): Uint8Array;
    deserializeKeyValue(key: Uint8Array | undefined, value: Uint8Array): KeyValue<K, V>;
}
export declare function makeStreamSerde<T>(serde: Serde<T>): StreamSerde<T>;
export declare function makeStreamKeyValueSerde<K, V>(keySerde: Serde<K>, valueSerde: Serde<V>): StreamSerde<KeyValue<K, V>>;
//# sourceMappingURL=stream.d.ts.map