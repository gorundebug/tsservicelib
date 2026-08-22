import { InputDataSource, Context, MessageContext, type Completion, type Consumer, type Metrics, type RuntimeEnvironment, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
export interface KafkaRecord {
    readonly topic: string;
    readonly partition: number;
    readonly offset: bigint;
    readonly key: Uint8Array | undefined;
    readonly value: Uint8Array;
    readonly headers: ReadonlyMap<string, Uint8Array>;
}
export interface KafkaConsumerControl {
    mark(record: KafkaRecord, metadata: string): void;
    commit(record: KafkaRecord): Promise<void>;
}
export declare class ConsumerMessage {
    #private;
    readonly key: Uint8Array | undefined;
    readonly value: Uint8Array;
    readonly topic: string;
    readonly partition: number;
    readonly offset: bigint;
    constructor(record: KafkaRecord, control: KafkaConsumerControl);
    markMessage(metadata?: string): void;
    commit(): Promise<void>;
}
export interface KafkaConsumer {
    connect(): Promise<void>;
    subscribe(topic: string): Promise<void>;
    run(concurrency: number, handler: (record: KafkaRecord, control: KafkaConsumerControl) => Promise<void>): Promise<void>;
    stop(): Promise<void>;
    disconnect(): Promise<void>;
}
export interface KafkaAdmin {
    connect(): Promise<void>;
    createTopic(topic: string, partitions: number, replicationFactor: number): Promise<void>;
    partitionCount(topic: string): Promise<number>;
    disconnect(): Promise<void>;
}
export interface KafkaClientFactory {
    consumer(brokers: readonly string[], groupId: string, connectionTimeoutMs: number, security?: KafkaSecurity): KafkaConsumer;
    admin(brokers: readonly string[], connectionTimeoutMs: number, security?: KafkaSecurity): KafkaAdmin;
}
interface KafkaSecurity {
    readonly protocol: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL";
    readonly mechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
    readonly username?: string | undefined;
    readonly password?: string | undefined;
}
export declare class ConfluentKafkaClientFactory implements KafkaClientFactory {
    #private;
    constructor(metrics?: Metrics);
    consumer(brokers: readonly string[], groupId: string, connectionTimeoutMs: number, security?: KafkaSecurity): KafkaConsumer;
    admin(brokers: readonly string[], connectionTimeoutMs: number, security?: KafkaSecurity): KafkaAdmin;
}
export type ResultCallback<HandlerState, T, R, E> = (context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<R>) => boolean | Promise<boolean>;
export interface ResultContext<HandlerState, T, R, E> {
    setResultCallback(messageId: string, callback: ResultCallback<HandlerState, T, R, E>): void;
    done(): void;
}
export interface EndpointHandler<HandlerState, T, R, E> {
    concurrency(stream: StreamContext<T, R, E>): number;
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, message: ConsumerMessage, result: ResultContext<HandlerState, T, R, E>): Completion;
    getMessageId(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<R>): string;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, handlerState: HandlerState): Completion;
}
export declare class KafkaDataSource extends InputDataSource {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment, factory: KafkaClientFactory);
    factory(): KafkaClientFactory;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private kafkaEndpoints;
}
export declare function makeKafkaEndpointConsumer<HandlerState, T, R, E>(stream: TypedInputStream<T, R, E>, handler: EndpointHandler<HandlerState, T, R, E>, factory?: KafkaClientFactory): Consumer<T>;
export {};
//# sourceMappingURL=confluent.d.ts.map