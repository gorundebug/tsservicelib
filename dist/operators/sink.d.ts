import { ConsumedStream, ServiceStream, type Completion, type Consumer, type MessageContext, type SinkStreamConfig, type TypedStream, type TypedStreamConsumer, type StreamSerde } from "../runtime/index.js";
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