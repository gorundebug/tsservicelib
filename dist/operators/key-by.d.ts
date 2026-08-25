import type { Collector } from "../runtime/collector.js";
import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { KeyByStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
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