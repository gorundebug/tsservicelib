import type { Context, MessageContext } from "./context.js";
import type { Collector } from "./collector.js";
import { RuntimeDataConnector, type DataConnector, type Endpoint } from "./data-connector.js";
import { type DataConnectorConfig, type EndpointConfig } from "./config/index.js";
import { type Logger, type RuntimeEnvironment } from "./environment/index.js";
import type { Lifecycle } from "./lifecycle.js";
import type { Completion, Consumer, TypedConsumedStream, TypedStream, TypedStreamConsumer } from "./stream.js";
/** Runtime boundary implemented structurally by the Sink operator. */
export interface TypedSinkStream<T, E> extends TypedStreamConsumer<T> {
    endpointId(): number;
    errorStream(): TypedConsumedStream<E>;
    setSinkConsumer(consumer: Consumer<T>): void;
}
/** Runtime boundary implemented structurally by the result-producing Sink operator. */
export interface TypedSinkStreamWithResult<T, R, E> extends TypedStream<R>, TypedStreamConsumer<T> {
    endpointId(): number;
    errorStream(): TypedConsumedStream<E>;
    setSinkConsumer(consumer: Consumer<T>): void;
    consumeResult(context: MessageContext, value: R): Completion;
}
/** Common typed collector context passed to datasink endpoint handlers. */
export declare class SinkStreamContext<T, R, E> {
    #private;
    readonly stream: TypedStream<R>;
    readonly logger: Logger;
    constructor(stream: TypedStream<R>, logger: Logger, collector: Collector<R>, errorCollector: Collector<E>);
    collect(context: MessageContext, value: R): Completion;
    errorCollect(context: MessageContext, value: E): Completion;
}
export declare function makeSinkStreamContext<T, R, E>(stream: TypedStream<R>, collector: Collector<R>, errorCollector: Collector<E>): SinkStreamContext<T, R, E>;
export interface DataSink extends DataConnector, Lifecycle {
    config(): DataConnectorConfig;
    runtimeEnvironment(): RuntimeEnvironment;
    addEndpoint(endpoint: SinkEndpoint): void;
    endpoint(id: number): SinkEndpoint | undefined;
    endpoints(): readonly SinkEndpoint[];
}
export interface SinkEndpoint extends Endpoint {
    dataSink(): DataSink;
    addEndpointConsumer(consumer: OutputEndpointConsumer): void;
    endpointConsumers(): readonly OutputEndpointConsumer[];
    onBeginRequestFailed(context: MessageContext, error: Error): void;
    onLateResult(context: MessageContext, streamId: string): void;
    onRequestStart(context: MessageContext): number | undefined;
    onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void;
}
export interface OutputEndpointConsumer {
    endpoint(): SinkEndpoint;
}
export declare abstract class OutputDataSink extends RuntimeDataConnector implements DataSink {
    #private;
    abstract start(context: Context): Promise<void>;
    abstract stop(context: Context): Promise<void>;
    addEndpoint(endpoint: SinkEndpoint): void;
    endpoint(id: number): SinkEndpoint | undefined;
    endpoints(): readonly SinkEndpoint[];
}
export declare class DataSinkEndpoint implements SinkEndpoint {
    #private;
    readonly id: number;
    readonly name: string;
    constructor(dataSink: DataSink, endpointId: number);
    config(): EndpointConfig;
    runtimeEnvironment(): RuntimeEnvironment;
    dataSink(): DataSink;
    dataConnector(): DataConnector;
    addEndpointConsumer(consumer: OutputEndpointConsumer): void;
    endpointConsumers(): readonly OutputEndpointConsumer[];
    onBeginRequestFailed(context: MessageContext, error: Error): void;
    onLateResult(context: MessageContext, streamId: string): void;
    onRequestStart(context: MessageContext): number | undefined;
    onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void;
}
export declare class DataSinkEndpointConsumer<T, E> implements OutputEndpointConsumer {
    #private;
    constructor(endpoint: SinkEndpoint, stream: TypedSinkStream<T, E>);
    endpoint(): SinkEndpoint;
    stream(): TypedSinkStream<T, E>;
}
export declare class DataSinkEndpointConsumerWithResult<T, R, E> implements OutputEndpointConsumer {
    #private;
    constructor(endpoint: SinkEndpoint, stream: TypedSinkStreamWithResult<T, R, E>);
    endpoint(): SinkEndpoint;
    stream(): TypedSinkStreamWithResult<T, R, E>;
}
//# sourceMappingURL=data-sink.d.ts.map