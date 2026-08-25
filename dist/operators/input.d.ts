import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { InputStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import type { Completion, Consumer, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import { ErrorStream } from "./error.js";
export declare class InputStream<T, R, E> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: InputStreamConfig, environment: RuntimeEnvironment, valueSerde: StreamSerde<T>, errorSerde: StreamSerde<E>);
    endpointId(): number;
    errorStream(): ErrorStream<E>;
    resultStream(): TypedStream<R> | undefined;
    setSource(source: TypedStream<R>): void;
    setResultConsumer(consumer: Consumer<R>): void;
    consume(context: MessageContext, value: T): Completion;
    consumeError(context: MessageContext, value: E): Completion;
    consumeResult(context: MessageContext, value: R): Completion;
    functionImplementation(): undefined;
}
export declare class InputKVStream<K, V, R, E> extends InputStream<KeyValue<K, V>, R, E> {
}
export declare function makeInputStream<T, R, E>(config: InputStreamConfig, environment: RuntimeEnvironment): InputStream<T, R, E>;
export declare function makeInputKVStream<K, V, R, E>(config: InputStreamConfig, environment: RuntimeEnvironment): InputKVStream<K, V, R, E>;
//# sourceMappingURL=input.d.ts.map