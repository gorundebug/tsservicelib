import { ConsumedStream, type JoinStorage, type KeyValue, type MessageContext, type MultiJoinStreamConfig, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
import type { MultiJoinFunction } from "./functions.js";
declare const bindMultiJoinRight: unique symbol;
export declare class MultiJoinStream<K, T, R> extends ConsumedStream<R> implements TypedStreamConsumer<KeyValue<K, T>> {
    #private;
    constructor(config: MultiJoinStreamConfig, left: TypedStream<KeyValue<K, T>>, function_: MultiJoinFunction<K, T, R>);
    functionImplementation(): MultiJoinFunction<K, T, R>;
    storage(): JoinStorage<K>;
    consume(context: MessageContext, value: KeyValue<K, T>): Promise<void>;
    consumeRight<V>(context: MessageContext, index: number, value: KeyValue<K, V>): Promise<void>;
    [bindMultiJoinRight]<V>(right: TypedStream<KeyValue<K, V>>): void;
    private consumeValue;
}
export declare function makeMultiJoinStream<K, T, R>(config: MultiJoinStreamConfig, left: TypedStream<KeyValue<K, T>>, function_: MultiJoinFunction<K, T, R>): MultiJoinStream<K, T, R>;
export declare function makeMultiJoinLink<K, T, V, R>(multiJoin: MultiJoinStream<K, T, R>, right: TypedStream<KeyValue<K, V>>): void;
export {};
//# sourceMappingURL=multi-join.d.ts.map