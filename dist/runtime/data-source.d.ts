import { type DataConnectorConfig, type EndpointConfig } from "./config/index.js";
import type { Collector } from "./collector.js";
import type { Context, MessageContext } from "./context.js";
import { RuntimeDataConnector, type DataConnector, type Endpoint } from "./data-connector.js";
import { type Logger, type RuntimeEnvironment } from "./environment/index.js";
import type { Lifecycle } from "./lifecycle.js";
import type { Completion, Consumer, TypedConsumedStream, TypedStream, TypedStreamConsumer } from "./stream.js";
/** Runtime boundary implemented structurally by the Input operator. */
export interface TypedInputStream<T, R, E> extends TypedStream<T>, TypedStreamConsumer<T> {
    endpointId(): number;
    errorStream(): TypedConsumedStream<E>;
    resultStream(): TypedStream<R> | undefined;
    setSource(source: TypedStream<R>): void;
    setResultConsumer(consumer: Consumer<R>): void;
    consumeError(context: MessageContext, value: E): Completion;
    consumeResult(context: MessageContext, value: R): Completion;
}
/** Apply the current reloadable source-endpoint tracing policy to one event. */
export declare function applyDataSourceEndpointTracing(context: MessageContext, environment: RuntimeEnvironment, endpointId: number): MessageContext;
/** Common typed collector context passed to datasource endpoint handlers. */
export declare class StreamContext<T, R, E> {
    #private;
    readonly stream: TypedStream<T>;
    readonly resultStream: TypedStream<R> | undefined;
    readonly logger: Logger;
    constructor(stream: TypedStream<T>, resultStream: TypedStream<R> | undefined, logger: Logger, collector: Collector<T>, errorCollector: Collector<E>);
    collect(context: MessageContext, value: T): Completion;
    errorCollect(context: MessageContext, value: E): Completion;
}
export declare function makeStreamContext<T, R, E>(stream: TypedStream<T>, resultStream: TypedStream<R> | undefined, collector: Collector<T>, errorCollector: Collector<E>): StreamContext<T, R, E>;
export interface DataSource extends DataConnector, Lifecycle {
    config(): DataConnectorConfig;
    runtimeEnvironment(): RuntimeEnvironment;
    addEndpoint(endpoint: InputEndpoint): void;
    endpoint(id: number): InputEndpoint | undefined;
    endpoints(): readonly InputEndpoint[];
}
export interface InputEndpoint extends Endpoint {
    dataSource(): DataSource;
    addEndpointConsumer(consumer: InputEndpointConsumer): void;
    endpointConsumers(): readonly InputEndpointConsumer[];
    onMissingStreamId(context: MessageContext): void;
    onLateResult(context: MessageContext, sessionId: string): void;
    onUnknownMessageId(context: MessageContext, sessionId: string, messageId: string): void;
    onDuplicateMessageId(context: MessageContext, sessionId: string, messageId: string): void;
    onPendingAdd(context: MessageContext, streamId: string): void;
    onPendingRemove(context: MessageContext, streamId: string): void;
    onInvalidHttpMethod(context: Context, method: string): void;
    onBeginRequestFailed(context: MessageContext, error: Error): void;
    onRequestStart(context: MessageContext): number | undefined;
    onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void;
}
export interface InputEndpointConsumer {
    endpoint(): InputEndpoint;
}
export declare abstract class InputDataSource extends RuntimeDataConnector implements DataSource {
    #private;
    abstract start(context: Context): Promise<void>;
    abstract stop(context: Context): Promise<void>;
    addEndpoint(endpoint: InputEndpoint): void;
    endpoint(id: number): InputEndpoint | undefined;
    endpoints(): readonly InputEndpoint[];
}
export declare class DataSourceEndpoint implements InputEndpoint {
    #private;
    readonly id: number;
    readonly name: string;
    constructor(dataSource: DataSource, endpointId: number);
    config(): EndpointConfig;
    runtimeEnvironment(): RuntimeEnvironment;
    dataSource(): DataSource;
    dataConnector(): DataConnector;
    addEndpointConsumer(consumer: InputEndpointConsumer): void;
    endpointConsumers(): readonly InputEndpointConsumer[];
    onMissingStreamId(context: MessageContext): void;
    onLateResult(context: MessageContext, sessionId: string): void;
    onUnknownMessageId(context: MessageContext, sessionId: string, messageId: string): void;
    onDuplicateMessageId(context: MessageContext, sessionId: string, messageId: string): void;
    onPendingAdd(context: MessageContext, streamId: string): void;
    onPendingRemove(context: MessageContext, streamId: string): void;
    onInvalidHttpMethod(context: Context, method: string): void;
    onBeginRequestFailed(context: MessageContext, error: Error): void;
    onRequestStart(context: MessageContext): number | undefined;
    onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void;
    private oldestPendingAge;
}
export declare class DataSourceEndpointConsumer<T, R, E> implements InputEndpointConsumer {
    #private;
    constructor(endpoint: InputEndpoint, stream: TypedInputStream<T, R, E>);
    endpoint(): InputEndpoint;
    stream(): TypedInputStream<T, R, E>;
    consume(context: MessageContext, value: T): void | Promise<void>;
}
//# sourceMappingURL=data-source.d.ts.map