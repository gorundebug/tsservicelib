import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { QueryStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import { type Completion, type Consumer, type TypedStream, type TypedStreamConsumer } from "../runtime/stream.js";
import { ErrorStream } from "./error.js";
export declare class QueryStream<T, R, E> extends ConsumedStream<R> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: QueryStreamConfig, source: TypedStream<T>);
    endpointId(): number;
    errorStream(): ErrorStream<E>;
    setQueryConsumer(consumer: Consumer<T>): void;
    inputSerde(): StreamSerde<T>;
    consume(context: MessageContext, value: T): Completion;
    consumeResult(context: MessageContext, value: R): Completion;
    functionImplementation(): undefined;
}
export declare function makeQueryStream<T, R, E>(config: QueryStreamConfig, source: TypedStream<T>): QueryStream<T, R, E>;
//# sourceMappingURL=query.d.ts.map