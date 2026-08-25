import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { SinkStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import { ServiceStream, type Completion, type Consumer, type TypedStream, type TypedStreamConsumer } from "../runtime/stream.js";
import { ErrorStream } from "./error.js";
export declare class SinkStream<T, E> extends ServiceStream implements TypedStreamConsumer<T> {
    #private;
    constructor(config: SinkStreamConfig, source: TypedStream<T>);
    endpointId(): number;
    errorStream(): ErrorStream<E>;
    setSinkConsumer(consumer: Consumer<T>): void;
    inputSerde(): StreamSerde<T>;
    consume(context: MessageContext, value: T): Completion;
    functionImplementation(): undefined;
}
export declare class SinkStreamWithResult<T, R, E> extends ConsumedStream<R> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: SinkStreamConfig, source: TypedStream<T>);
    endpointId(): number;
    errorStream(): ErrorStream<E>;
    setSinkConsumer(consumer: Consumer<T>): void;
    inputSerde(): StreamSerde<T>;
    consume(context: MessageContext, value: T): Completion;
    consumeResult(context: MessageContext, value: R): Completion;
    functionImplementation(): undefined;
}
export declare function makeSinkStream<T, E>(config: SinkStreamConfig, source: TypedStream<T>): SinkStream<T, E>;
export declare function makeSinkStreamWithResult<T, R, E>(config: SinkStreamConfig, source: TypedStream<T>): SinkStreamWithResult<T, R, E>;
//# sourceMappingURL=sink.d.ts.map