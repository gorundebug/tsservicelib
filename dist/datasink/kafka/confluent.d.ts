import { OutputDataSink, type Completion, type Consumer, type Context, type MessageContext, type Metrics, type RuntimeEnvironment, type Stream, type TypedSinkStream } from "../../runtime/index.js";
export interface DeliveryResult {
    readonly partition: number;
    readonly offset: bigint;
}
export interface KafkaProducer {
    connect(): Promise<void>;
    send(topic: string, key: Uint8Array | undefined, value: Uint8Array, partition?: number): Promise<DeliveryResult>;
    flush(timeoutMs?: number): Promise<void>;
    disconnect(): Promise<void>;
}
export interface KafkaAdmin {
    connect(): Promise<void>;
    createTopic(topic: string, partitions: number, replicationFactor: number): Promise<void>;
    partitionCount(topic: string): Promise<number>;
    disconnect(): Promise<void>;
}
export interface KafkaClientFactory {
    producer(brokers: readonly string[], connectionTimeoutMs: number, security?: KafkaSecurity): KafkaProducer;
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
    producer(brokers: readonly string[], connectionTimeoutMs: number, security?: KafkaSecurity): KafkaProducer;
    admin(brokers: readonly string[], connectionTimeoutMs: number, security?: KafkaSecurity): KafkaAdmin;
}
export interface Partitioner<T> {
    partition(value: Readonly<T>, partitions: number): number | Promise<number>;
}
export type DeliveryCallback<R> = (partition: number, offset: bigint, error: Error | undefined) => R | undefined;
export declare class SinkMessage<R> {
    #private;
    key: Uint8Array | undefined;
    value: Uint8Array;
    constructor(topic: string, send: (key: Uint8Array | undefined, value: Uint8Array, onDelivery: (result: DeliveryResult | undefined, error: Error | undefined) => Completion) => void, result: (context: MessageContext, value: R) => Completion);
    topic(): string;
    send(context: MessageContext, onDelivery: DeliveryCallback<R>): void;
    sendSync(context: MessageContext): Promise<DeliveryResult>;
    out(context: MessageContext, value: R): Completion;
    skip(context: MessageContext, value: R): Completion;
}
export interface EndpointHandler<HandlerState, T, R> {
    getStreamId(context: MessageContext, value: Readonly<T>): string;
    beginRequest(context: MessageContext, stream: Stream): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: Stream, handlerState: HandlerState, value: Readonly<T>, message: SinkMessage<R>): Completion;
    endRequest(context: MessageContext, stream: Stream, error: Error | undefined, handlerState: HandlerState): Completion;
}
export declare class KafkaDataSink extends OutputDataSink {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment, factory: KafkaClientFactory);
    factory(): KafkaClientFactory;
    producer(): KafkaProducer;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    send(context: MessageContext, topic: string, key: Uint8Array | undefined, value: Uint8Array, partition: () => Promise<number | undefined>, onDelivery: (result: DeliveryResult | undefined, error: Error | undefined) => Completion): void;
    private recoverProducer;
    private replaceProducer;
    private kafkaEndpoints;
}
export declare function makeKafkaEndpointConsumer<HandlerState, T, R>(stream: TypedSinkStream<T, R>, handler: EndpointHandler<HandlerState, T, R>, factory?: KafkaClientFactory): Consumer<T>;
export {};
//# sourceMappingURL=confluent.d.ts.map