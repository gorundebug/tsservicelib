import { ConsumedStream } from "../runtime/consumed-stream.js";
import { type JoinStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import { type JoinStorage } from "../runtime/store/join-storage.js";
import type { TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { JoinFunction } from "./functions.js";
export declare class JoinStream<K, L, R, O> extends ConsumedStream<O> implements TypedStreamConsumer<KeyValue<K, L>> {
    #private;
    constructor(config: JoinStreamConfig, left: TypedStream<KeyValue<K, L>>, right: TypedStream<KeyValue<K, R>>, function_: JoinFunction<K, L, R, O>);
    functionImplementation(): JoinFunction<K, L, R, O>;
    storage(): JoinStorage<K>;
    consume(context: MessageContext, value: KeyValue<K, L>): Promise<void>;
    consumeRight(context: MessageContext, value: KeyValue<K, R>): Promise<void>;
    private consumeValue;
    private callFunction;
}
export declare function makeJoinStream<K, L, R, O>(config: JoinStreamConfig, left: TypedStream<KeyValue<K, L>>, right: TypedStream<KeyValue<K, R>>, function_: JoinFunction<K, L, R, O>): JoinStream<K, L, R, O>;
//# sourceMappingURL=join.d.ts.map