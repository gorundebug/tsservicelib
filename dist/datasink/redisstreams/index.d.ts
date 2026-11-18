import { PublicationMetrics } from "../../internal/broker-publication-metrics.js";
import { OutputDataSink, DataSinkEndpoint, type Context, type MessageContext, type RuntimeEnvironment, type Completion, type Consumer, type TypedSinkStream, type TypedSinkStreamWithResult, type Stream } from "../../runtime/index.js";
import { type Delivery, type Fields } from "../../internal/redis/client.js";
export type { Delivery, Fields, FieldValue } from "../../internal/redis/client.js";
export type DeliveryCallback<R> = (delivery: Delivery | undefined, error: Error | undefined) => R | undefined | Promise<R | undefined>;
/** Typed native sink context; plain sinks expose errors without inventing result streams. */
export declare class StreamContext<T, R, E> {
    readonly stream: Stream;
    private readonly output;
    private readonly errors;
    readonly logger: ReturnType<RuntimeEnvironment["log"]>;
    constructor(stream: Stream, output: (context: MessageContext, value: R) => Completion, errors: (context: MessageContext, value: E) => Completion);
    collect(context: MessageContext, value: R): Completion;
    errorCollect(context: MessageContext, value: E): Completion;
}
export interface EndpointHandler<H, T, R, E> {
    getStreamId(context: MessageContext, value: Readonly<T>): string;
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        context: MessageContext;
        state: H;
    } | Promise<{
        context: MessageContext;
        state: H;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, state: H, value: Readonly<T>, message: SinkMessage<R>): Completion;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, state: H): Completion;
}
export declare class SinkMessage<R> {
    #private;
    private readonly ep;
    private readonly output;
    retire(): void;
    fields: Fields;
    constructor(ep: SinkEndpoint, output: (context: MessageContext, result: R) => Completion);
    /** Returns after bounded admission; callback follows native publication outcome. */
    send(context: MessageContext, onDelivery: DeliveryCallback<R>): void;
    /** Await the same native outcome; does not emit a graph result implicitly. */
    sendSync(context: MessageContext): Promise<Delivery>;
    out(context: MessageContext, result: R): Completion;
    skip(context: MessageContext, result: R): Completion;
}
declare class SinkEndpoint extends DataSinkEndpoint {
    #private;
    readonly publication: PublicationMetrics;
    start(parent: Context): Promise<void>;
    report(context: Context, error: Error): void;
    runHandler(context: MessageContext, handler: (context: MessageContext) => Promise<void>): Promise<void>;
    send(context: MessageContext, fields: Fields, callback: (delivery: Delivery | undefined, error: Error | undefined) => Completion): void;
    sendSync(context: MessageContext, fields: Fields): Promise<Delivery>;
    stopAdmission(): void;
    stop(context: Context): Promise<void>;
}
export declare class RedisStreamsDataSink extends OutputDataSink {
    constructor(id: number, environment: RuntimeEnvironment);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
}
export declare function makeRedisStreamsEndpointConsumer<H, T, E>(stream: TypedSinkStream<T, E>, handler: EndpointHandler<H, T, never, E>): Consumer<T>;
export declare function makeRedisStreamsEndpointConsumerWithResult<H, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, handler: EndpointHandler<H, T, R, E>): Consumer<T>;
//# sourceMappingURL=index.d.ts.map