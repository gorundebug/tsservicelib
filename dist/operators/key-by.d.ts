import { ConsumedStream, type Collector, type Completion, type KeyValue, type MessageContext, type KeyByStreamConfig, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
import type { KeyByFunction } from "./functions.js";
export declare class KeyByStream<T, K, V> extends ConsumedStream<KeyValue<K, V>> implements TypedStreamConsumer<T>, Collector<KeyValue<K, V>> {
    #private;
    constructor(config: KeyByStreamConfig, source: TypedStream<T>, function_: KeyByFunction<T, K, V>);
    source(): TypedStream<T>;
    functionImplementation(): KeyByFunction<T, K, V>;
    consume(context: MessageContext, value: T): Completion;
}
export declare function makeKeyByStream<T, K, V>(config: KeyByStreamConfig, source: TypedStream<T>, function_: KeyByFunction<T, K, V>): KeyByStream<T, K, V>;
//# sourceMappingURL=key-by.d.ts.map