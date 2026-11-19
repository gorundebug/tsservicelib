export type Properties = Readonly<Record<string, unknown>>;
export type KubernetesWorkloadType = "Deployment" | "StatefulSet";
export interface NamedIdentity {
    readonly id: number;
    readonly name: string;
    readonly properties: Properties;
}
export interface ServiceConfig extends NamedIdentity {
    readonly color: string;
    readonly defaultCallSemantics?: CallSemanticsGroup | undefined;
    readonly environment: string;
    readonly golangVersion?: string | undefined;
    readonly grpcHost: string;
    readonly grpcPort: number;
    readonly httpHost: string;
    readonly httpPort: number;
    readonly defaultGrpcTimeout?: number | undefined;
    readonly logLevel?: string | undefined;
    readonly metricsHandler: string;
    readonly startupHandler: string;
    readonly readinessHandler: string;
    readonly livenessHandler: string;
    readonly kubernetesWorkloadType: KubernetesWorkloadType;
    readonly modulePath?: string | undefined;
    readonly shutdownTimeout: number;
    readonly statusHandler: string;
}
export type TransformationType = "Case" | "CycleLink" | "Delay" | "Error" | "Filter" | "FlatMap" | "FlatMapIterable" | "Input" | "SubStream" | "Join" | "KeyBy" | "Map" | "Merge" | "MultiJoin" | "Process" | "Sink" | "Query" | "Split" | "When";
export type TransformationName = "case" | "cycleLink" | "delay" | "error" | "filter" | "flatMap" | "flatMapIterable" | "input" | "substream" | "join" | "keyBy" | "map" | "merge" | "multiJoin" | "process" | "sink" | "query" | "split" | "when";
export declare function transformationName(type: TransformationType): TransformationName;
export interface StreamConfig extends NamedIdentity {
    readonly type: TransformationType;
    readonly pipeline: string;
    readonly component?: string | undefined;
    readonly idService: number;
    readonly idSource: number;
    readonly idSources: readonly number[];
    readonly xPos: number;
    readonly yPos: number;
}
export interface FunctionConfig {
    readonly functionPackage?: string | undefined;
    readonly functionName?: string | undefined;
    readonly publicFunction?: boolean | undefined;
    readonly functionDescription?: string | undefined;
    readonly functionInitializerGroup?: string | undefined;
    readonly functionModule?: string | undefined;
}
export interface MapStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "Map";
    readonly valueType: string;
}
export interface MergeStreamConfig extends StreamConfig {
    readonly type: "Merge";
}
export interface FilterStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "Filter";
}
export interface DelayStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "Delay";
    readonly duration: number;
}
export interface FlatMapStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "FlatMap";
    readonly valueType: string;
}
export interface FlatMapIterableStreamConfig extends StreamConfig {
    readonly type: "FlatMapIterable";
    readonly valueType: string;
}
export interface InputStreamConfig extends StreamConfig {
    readonly type: "Input";
    readonly valueType: string;
    readonly idEndpoint: number;
}
export interface SubStreamConfig extends StreamConfig {
    readonly type: "SubStream";
    readonly valueType: string;
}
export declare const JoinType: {
    readonly Undefined: 0;
    readonly Inner: 1;
    readonly Left: 2;
    readonly Right: 3;
    readonly Outer: 4;
};
export type JoinType = (typeof JoinType)[keyof typeof JoinType];
export declare const JoinStorageType: {
    readonly Undefined: 0;
    readonly HashMap: 1;
    readonly RocksDB: 2;
    readonly Aerospike: 3;
};
export type JoinStorageType = (typeof JoinStorageType)[keyof typeof JoinStorageType];
export interface JoinStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "Join";
    readonly valueType: string;
    readonly joinType: JoinType;
    readonly joinStorage: JoinStorageType;
    readonly ttl: number;
    readonly renewTTL: boolean;
}
export interface MultiJoinStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "MultiJoin";
    readonly valueType: string;
    readonly joinStorage: JoinStorageType;
    readonly ttl: number;
    readonly renewTTL: boolean;
}
export interface KeyByStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "KeyBy";
    readonly keyType: string;
    readonly valueType: string;
}
export interface ProcessStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "Process";
    readonly pattern?: string | undefined;
}
export interface SinkStreamConfig extends StreamConfig {
    readonly type: "Sink";
    readonly valueType?: string | undefined;
    readonly idEndpoint: number;
}
export interface QueryStreamConfig extends StreamConfig {
    readonly type: "Query";
    readonly valueType?: string | undefined;
    readonly idEndpoint: number;
}
export interface CaseStreamConfig extends StreamConfig, FunctionConfig {
    readonly type: "Case";
}
export interface WhenStreamConfig extends StreamConfig {
    readonly type: "When";
    readonly valueType: string;
}
export interface SplitStreamConfig extends StreamConfig {
    readonly type: "Split";
}
export interface CycleLinkStreamConfig extends StreamConfig {
    readonly type: "CycleLink";
}
export type AnyStreamConfig = InputStreamConfig | SubStreamConfig | MapStreamConfig | FilterStreamConfig | JoinStreamConfig | MultiJoinStreamConfig | ProcessStreamConfig | FlatMapStreamConfig | FlatMapIterableStreamConfig | KeyByStreamConfig | MergeStreamConfig | SplitStreamConfig | CaseStreamConfig | SinkStreamConfig | QueryStreamConfig | CycleLinkStreamConfig | DelayStreamConfig | WhenStreamConfig;
export interface DataConnectorConfig extends NamedIdentity {
    readonly type: DataConnectorType;
    readonly implementation: string;
}
export declare const DataConnectorType: {
    readonly Undefined: 0;
    readonly HTTP: 1;
    readonly GRPC: 2;
    readonly Kafka: 3;
    readonly Custom: 4;
    readonly Cron: 5;
    readonly Temporal: 6;
    readonly NATS: 7;
    readonly JetStream: 8;
    readonly RabbitMQ: 9;
    readonly RedisStreams: 10;
    readonly Pulsar: 11;
    readonly PostgreSQLKysely: 13;
    readonly S3AWS: 18;
};
export type DataConnectorType = (typeof DataConnectorType)[keyof typeof DataConnectorType];
export interface HttpDataConnectorConfig extends DataConnectorConfig {
    readonly module?: string | undefined;
    readonly host?: string | undefined;
    readonly port?: number | undefined;
    readonly useDedicatedListener: boolean;
}
export interface GrpcDataConnectorConfig extends DataConnectorConfig {
    readonly programmingLanguage?: number | undefined;
    readonly module?: string | undefined;
    readonly address?: string | undefined;
    readonly connectionsCount: number;
}
export interface KafkaDataConnectorConfig extends DataConnectorConfig {
    readonly programmingLanguage?: number | undefined;
    readonly brokers: string;
    readonly version?: string | undefined;
    readonly dialTimeout: number;
    readonly usePartitioner: boolean;
    readonly async: boolean;
    readonly securityProtocol: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL";
    readonly saslMechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
    readonly username?: string | undefined;
    readonly password?: string | undefined;
}
export interface NATSDataConnectorConfig extends DataConnectorConfig {
    readonly type: 7 | 8;
    readonly servers: readonly string[];
    /** Integer milliseconds, default 2000. */
    readonly connectTimeoutMs: number;
    /** Integer milliseconds, default 5000. */
    readonly operationTimeoutMs: number;
    readonly username?: string | undefined;
    readonly password?: string | undefined;
    readonly token?: string | undefined;
    readonly credentialsFile?: string | undefined;
    readonly tlsEnabled?: boolean | undefined;
    readonly tlsServerName?: string | undefined;
    readonly tlsCaFile?: string | undefined;
    readonly tlsCertFile?: string | undefined;
    readonly tlsKeyFile?: string | undefined;
}
export interface NATSEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly subject: string;
    readonly queueGroup?: string | undefined;
    readonly streamName?: string | undefined;
    readonly durableName?: string | undefined;
    readonly createStream: boolean;
    readonly createConsumer: boolean;
}
export interface CustomDataConnectorConfig extends DataConnectorConfig {
    readonly type: typeof DataConnectorType.Custom;
}
export interface CronDataConnectorConfig extends DataConnectorConfig {
    readonly type: typeof DataConnectorType.Cron;
}
export interface TemporalDataConnectorConfig extends DataConnectorConfig {
    readonly type: typeof DataConnectorType.Temporal;
    readonly address: string;
    readonly namespace: string;
    readonly identity: string;
    readonly apiKey: string;
    readonly tlsEnabled: boolean;
    readonly tlsServerName: string;
    readonly tlsCaFile: string;
    readonly tlsCertFile: string;
    readonly tlsKeyFile: string;
    readonly workerStopTimeout: number;
}
export type AnyDataConnectorConfig = PostgreSQLKyselyDataConnectorConfig | S3AWSDataConnectorConfig | HttpDataConnectorConfig | GrpcDataConnectorConfig | KafkaDataConnectorConfig | NATSDataConnectorConfig | RabbitMQDataConnectorConfig | RedisStreamsDataConnectorConfig | PulsarDataConnectorConfig | CronDataConnectorConfig | TemporalDataConnectorConfig | CustomDataConnectorConfig;
export interface EndpointConfig extends NamedIdentity {
    readonly idDataConnector: number;
    readonly tracingEnabled?: boolean | undefined;
}
export declare const HTTPMethodType: {
    readonly Undefined: "";
    readonly GET: "GET";
    readonly POST: "POST";
    readonly PUT: "PUT";
    readonly PATCH: "PATCH";
    readonly DELETE: "DELETE";
    readonly HEAD: "HEAD";
    readonly OPTIONS: "OPTIONS";
    readonly TRACE: "TRACE";
    readonly CONNECT: "CONNECT";
};
export type HTTPMethodType = (typeof HTTPMethodType)[keyof typeof HTTPMethodType];
export interface HttpEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly httpMethodType: HTTPMethodType;
    readonly path: string;
}
export type GrpcMethodType = "NoStreaming" | "ClientStreaming" | "ServerStreaming" | "BidirectionalStreaming";
export interface GrpcEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly grpcMethodType: GrpcMethodType;
    readonly methodName: string;
}
export interface KafkaEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly createTopic: boolean;
    readonly topic: string;
    readonly partitions: number;
    readonly consumerGroup: string;
    readonly replicationFactor: number;
}
export type ScheduleOverlapPolicy = "Allow" | "Skip";
export type ScheduleMissedRunPolicy = "FireOnce" | "Skip";
export type TemporalExecutionType = "Activity" | "Workflow";
export interface CronEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly schedule: string;
    readonly timezone: string;
    readonly overlapPolicy: ScheduleOverlapPolicy;
    readonly missedRunPolicy: ScheduleMissedRunPolicy;
}
export interface TemporalEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly taskQueue: string;
    readonly temporalExecutionType: TemporalExecutionType;
    readonly maxConcurrentActivities: number;
    readonly maxConcurrentWorkflowTasks: number;
    readonly schedule: string;
    readonly scheduleId: string;
    readonly timezone: string;
    readonly overlapPolicy: ScheduleOverlapPolicy;
    readonly missedRunPolicy: ScheduleMissedRunPolicy;
    readonly workflowExecutionTimeout: number;
    readonly activityStartToCloseTimeout: number;
    readonly activityHeartbeatTimeout: number;
    readonly maximumAttempts: number;
}
export type CustomEndpointConfig = EndpointConfig & FunctionConfig & {
    readonly enabled: boolean;
};
export type AnyEndpointConfig = QueryEndpointConfig | HttpEndpointConfig | GrpcEndpointConfig | KafkaEndpointConfig | NATSEndpointConfig | RabbitMQEndpointConfig | RedisStreamsEndpointConfig | PulsarEndpointConfig | CronEndpointConfig | TemporalEndpointConfig | CustomEndpointConfig;
export interface PoolConfig {
    readonly name: string;
    readonly executorsCount: number;
    readonly queueCapacity: number;
    readonly properties: Properties;
}
export interface FunctionCallSemanticsConfig {
    readonly async: boolean;
}
export interface TaskPoolCallSemanticsConfig {
    readonly poolName: string;
}
export interface PriorityTaskPoolCallSemanticsConfig {
    readonly poolName: string;
    readonly priority: number;
}
export type ParallelCallSemanticsConfig = Readonly<Record<string, never>>;
export type CallSemanticsGroup = {
    readonly functionCall: FunctionCallSemanticsConfig;
} | {
    readonly taskPool: TaskPoolCallSemanticsConfig;
} | {
    readonly priorityTaskPool: PriorityTaskPoolCallSemanticsConfig;
} | {
    readonly parallelCall: ParallelCallSemanticsConfig;
};
export interface LinkConfig {
    readonly from: number;
    readonly to: number;
    readonly callSemantics?: CallSemanticsGroup | undefined;
    readonly properties: Properties;
}
export interface ModuleConfig {
    readonly name: string;
    readonly path: string;
    readonly properties: Properties;
}
export interface TypeConfig {
    readonly name: string;
    readonly type: string;
    readonly typeDefinition?: string | undefined;
    readonly typeImport?: string | undefined;
    readonly valueType?: string | undefined;
    readonly keyType?: string | undefined;
    readonly package?: string | undefined;
    readonly module?: string | undefined;
    readonly definitionFormat?: number | undefined;
    readonly publicType: boolean;
    readonly transferByValue: boolean;
    readonly useAlias: boolean;
    readonly properties: Properties;
}
export interface CanonicalConfig<TDataConnector extends DataConnectorConfig = DataConnectorConfig, TEndpoint extends EndpointConfig = EndpointConfig> {
    readonly services: readonly ServiceConfig[];
    readonly streams: readonly StreamConfig[];
    readonly dataConnectors: readonly TDataConnector[];
    readonly endpoints: readonly TEndpoint[];
    readonly pools: readonly PoolConfig[];
    readonly links: readonly LinkConfig[];
    readonly modules: readonly ModuleConfig[];
    readonly types: readonly TypeConfig[];
    readonly properties: Properties;
}
/**
 * Strongly typed shape of the generated YAML/JSON document before parsing.
 *
 * Document enums intentionally accept their serialized numeric values while
 * runtime configs above expose normalized semantic names. Unknown keys remain
 * available for canonical custom properties, but every known field is checked.
 */
export interface ConfigDocumentIdentity {
    readonly id: number;
    readonly name: string;
    readonly [property: string]: unknown;
}
export type CallSemanticsDocument = 0 | 1 | 2 | 3 | 4 | 5 | 6 | CallSemanticsGroup;
export interface ServiceConfigDocument extends ConfigDocumentIdentity {
    readonly color: string;
    readonly defaultCallSemantics?: CallSemanticsDocument | undefined;
    readonly defaultGrpcTimeout?: number | undefined;
    readonly environment: string;
    readonly golangVersion?: string | undefined;
    readonly grpcHost: string;
    readonly grpcPort: number;
    readonly httpHost: string;
    readonly httpPort: number;
    readonly logLevel?: string | undefined;
    readonly metricsHandler: string;
    readonly startupHandler: string;
    readonly readinessHandler: string;
    readonly livenessHandler: string;
    readonly kubernetesWorkloadType: KubernetesWorkloadType;
    readonly modulePath?: string | undefined;
    readonly shutdownTimeout: number;
    readonly statusHandler: string;
}
export interface FunctionConfigDocument {
    readonly functionPackage?: string | undefined;
    readonly functionName?: string | undefined;
    readonly publicFunction?: boolean | undefined;
    readonly functionDescription?: string | undefined;
    readonly functionInitializerGroup?: string | undefined;
    readonly functionModule?: string | undefined;
}
interface StreamConfigDocumentBase extends ConfigDocumentIdentity {
    readonly pipeline: string;
    readonly component?: string | undefined;
    readonly idService: number;
    readonly idSource?: number | undefined;
    readonly idSources?: readonly number[] | undefined;
    readonly xPos: number;
    readonly yPos: number;
}
export interface SubStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 18 | "SubStream";
    readonly valueType: string;
}
export interface InputStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 1 | "Input";
    readonly valueType: string;
    readonly idEndpoint: number;
}
export interface MapStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 2 | "Map";
    readonly valueType: string;
}
export interface FilterStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 3 | "Filter";
}
export interface JoinStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 4 | "Join";
    readonly valueType: string;
    readonly joinType: JoinType;
    readonly joinStorage: JoinStorageType;
    readonly ttl: number;
    readonly renewTTL: boolean;
}
export interface MultiJoinStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 5 | "MultiJoin";
    readonly valueType: string;
    readonly joinStorage: JoinStorageType;
    readonly ttl: number;
    readonly renewTTL: boolean;
}
export interface ProcessStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 6 | "Process";
    readonly pattern?: string | undefined;
}
export interface FlatMapStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 7 | "FlatMap";
    readonly valueType: string;
}
export interface FlatMapIterableStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 8 | "FlatMapIterable";
    readonly valueType: string;
}
export interface KeyByStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 9 | "KeyBy";
    readonly keyType: string;
    readonly valueType: string;
}
export interface MergeStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 10 | "Merge";
}
export interface SplitStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 11 | "Split";
}
export interface CaseStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 12 | "Case";
}
export interface SinkStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 13 | "Sink";
    readonly valueType?: string | undefined;
    readonly idEndpoint: number;
}
export interface QueryStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 19 | "Query";
    readonly valueType?: string | undefined;
    readonly idEndpoint: number;
}
export interface CycleLinkStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 14 | "CycleLink";
}
export interface DelayStreamConfigDocument extends StreamConfigDocumentBase, FunctionConfigDocument {
    readonly type: 16 | "Delay";
    readonly duration: number;
}
export interface WhenStreamConfigDocument extends StreamConfigDocumentBase {
    readonly type: 17 | "When";
    readonly valueType: string;
}
interface DataConnectorConfigDocumentBase extends ConfigDocumentIdentity {
    readonly implementation: string;
}
export interface HttpDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 1;
    readonly module?: string | undefined;
    readonly host?: string | undefined;
    readonly port?: number | undefined;
    readonly useDedicatedListener?: boolean | undefined;
}
export interface GrpcDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 2;
    readonly programmingLanguage?: number | undefined;
    readonly module?: string | undefined;
    readonly address?: string | undefined;
    readonly connectionsCount?: number | undefined;
}
export interface KafkaDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 3;
    readonly programmingLanguage?: number | undefined;
    readonly brokers?: string | undefined;
    readonly version?: string | undefined;
    readonly dialTimeout?: number | undefined;
    readonly usePartitioner?: boolean | undefined;
    readonly async?: boolean | undefined;
    readonly securityProtocol?: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL" | undefined;
    readonly saslMechanism?: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | undefined;
    readonly username?: string | undefined;
    readonly password?: string | undefined;
}
export interface CustomDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 4;
}
export interface CronDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 5;
}
export interface TemporalDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 6;
    readonly address?: string | undefined;
    readonly namespace?: string | undefined;
    readonly identity?: string | undefined;
    readonly apiKey?: string | undefined;
    readonly tlsEnabled?: boolean | undefined;
    readonly tlsServerName?: string | undefined;
    readonly tlsCaFile?: string | undefined;
    readonly tlsCertFile?: string | undefined;
    readonly tlsKeyFile?: string | undefined;
    readonly workerStopTimeout?: number | undefined;
}
interface EndpointConfigDocumentBase extends ConfigDocumentIdentity, FunctionConfigDocument {
    readonly idDataConnector: number;
    readonly tracingEnabled?: boolean | undefined;
}
export interface HttpEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly httpMethodType: "GET" | "POST";
    readonly path: string;
}
export interface GrpcEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly grpcMethodType: 1 | 2 | 4 | 5 | GrpcMethodType;
    readonly methodName: string;
}
export interface KafkaEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly createTopic?: boolean | undefined;
    readonly topic?: string | undefined;
    readonly partitions?: number | undefined;
    readonly consumerGroup?: string | undefined;
    readonly replicationFactor?: number | undefined;
}
export interface CronEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly schedule?: string | undefined;
    readonly timezone?: string | undefined;
    readonly overlapPolicy?: ScheduleOverlapPolicy | undefined;
    readonly missedRunPolicy?: ScheduleMissedRunPolicy | undefined;
}
export interface TemporalEndpointConfigDocument extends CronEndpointConfigDocument {
    readonly taskQueue?: string | undefined;
    readonly temporalExecutionType?: TemporalExecutionType | undefined;
    readonly scheduleId?: string | undefined;
    readonly workflowExecutionTimeout?: number | undefined;
    readonly activityStartToCloseTimeout?: number | undefined;
    readonly activityHeartbeatTimeout?: number | undefined;
    readonly maximumAttempts?: number | undefined;
    readonly maxConcurrentActivities?: number | undefined;
    readonly maxConcurrentWorkflowTasks?: number | undefined;
}
export type CustomEndpointConfigDocument = EndpointConfigDocumentBase & {
    readonly enabled?: boolean | undefined;
};
export interface PoolConfigDocument {
    readonly name: string;
    readonly executorsCount: number;
    readonly queueCapacity?: number | undefined;
    readonly [property: string]: unknown;
}
export interface LinkConfigDocument {
    readonly from: number;
    readonly to: number;
    readonly callSemantics?: CallSemanticsDocument | undefined;
    readonly poolName?: string | undefined;
    readonly priority?: number | undefined;
    readonly async?: boolean | undefined;
    readonly idDataConnector?: number | undefined;
    readonly [property: string]: unknown;
}
export interface ModuleConfigDocument {
    readonly name: string;
    readonly path: string;
    readonly [property: string]: unknown;
}
export interface TypeConfigDocument {
    readonly name: string;
    readonly type: string;
    readonly typeDefinition?: string | undefined;
    readonly typeImport?: string | undefined;
    readonly valueType?: string | undefined;
    readonly keyType?: string | undefined;
    readonly package?: string | undefined;
    readonly module?: string | undefined;
    readonly definitionFormat?: number | undefined;
    readonly publicType?: boolean | undefined;
    readonly transferByValue?: boolean | undefined;
    readonly useAlias?: boolean | undefined;
    readonly [property: string]: unknown;
}
export interface NATSDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly servers?: readonly string[] | undefined;
    readonly connectTimeoutMs?: number | undefined;
    readonly operationTimeoutMs?: number | undefined;
    readonly username?: string | undefined;
    readonly password?: string | undefined;
    readonly token?: string | undefined;
    readonly credentialsFile?: string | undefined;
    readonly tlsEnabled?: boolean | undefined;
    readonly tlsServerName?: string | undefined;
    readonly tlsCaFile?: string | undefined;
    readonly tlsCertFile?: string | undefined;
    readonly tlsKeyFile?: string | undefined;
}
export interface NATSEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly subject?: string | undefined;
    readonly queueGroup?: string | undefined;
    readonly streamName?: string | undefined;
    readonly durableName?: string | undefined;
    readonly createStream?: boolean | undefined;
    readonly createConsumer?: boolean | undefined;
}
export interface RabbitMQDataConnectorConfig extends Omit<NATSDataConnectorConfig, "type"> {
    readonly type: 9;
    readonly virtualHost?: string | undefined;
}
export interface RabbitMQEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly queueName?: string | undefined;
    readonly queueType?: "quorum" | "classic" | undefined;
    readonly exchangeName?: string | undefined;
    readonly exchangeType?: "direct" | "topic" | "fanout" | undefined;
    readonly routingKey?: string | undefined;
    readonly createQueue: boolean;
    readonly createExchange: boolean;
    readonly createBinding: boolean;
    readonly consumerCredits?: number | undefined;
    readonly publisherMaxInFlight?: number | undefined;
}
export interface RabbitMQDataConnectorConfigDocument extends NATSDataConnectorConfigDocument {
    readonly type: 9;
    readonly virtualHost?: string | undefined;
}
export interface RabbitMQEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly queueName?: string | undefined;
    readonly queueType?: "quorum" | "classic" | undefined;
    readonly exchangeName?: string | undefined;
    readonly exchangeType?: "direct" | "topic" | "fanout" | undefined;
    readonly routingKey?: string | undefined;
    readonly createQueue?: boolean | undefined;
    readonly createExchange?: boolean | undefined;
    readonly createBinding?: boolean | undefined;
    readonly consumerCredits?: number | undefined;
    readonly publisherMaxInFlight?: number | undefined;
}
export interface RedisStreamsDataConnectorConfig extends Omit<NATSDataConnectorConfig, "type"> {
    readonly type: 10;
    readonly redisDatabase: number;
}
export interface RedisStreamsEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly streamKey: string;
    readonly consumerGroup?: string | undefined;
    readonly consumerName?: string | undefined;
    readonly createConsumerGroup?: boolean | undefined;
    readonly groupStartId?: string | undefined;
    readonly readCount?: number | undefined;
    readonly readBlockMs?: number | undefined;
    readonly claimIdleMs?: number | undefined;
    readonly claimIntervalMs?: number | undefined;
    readonly createStream?: boolean | undefined;
    readonly publisherMaxInFlight?: number | undefined;
}
export interface RedisStreamsDataConnectorConfigDocument extends NATSDataConnectorConfigDocument {
    readonly type: 10;
    readonly redisDatabase?: number | undefined;
}
export interface RedisStreamsEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly streamKey: string;
    readonly consumerGroup?: string | undefined;
    readonly consumerName?: string | undefined;
    readonly createConsumerGroup?: boolean | undefined;
    readonly groupStartId?: string | undefined;
    readonly readCount?: number | undefined;
    readonly readBlockMs?: number | undefined;
    readonly claimIdleMs?: number | undefined;
    readonly claimIntervalMs?: number | undefined;
    readonly createStream?: boolean | undefined;
    readonly publisherMaxInFlight?: number | undefined;
}
/** Draft Pulsar configuration; native runtime acceptance is tracked separately. */
export interface PulsarDataConnectorConfig extends DataConnectorConfig {
    readonly type: 11;
    readonly servers: readonly string[];
    readonly adminUrl: string;
    readonly adminToken?: string | undefined;
    readonly connectTimeoutMs: number;
    readonly operationTimeoutMs: number;
    readonly token?: string | undefined;
    readonly tlsCaFile?: string | undefined;
    readonly tlsCertFile?: string | undefined;
    readonly tlsKeyFile?: string | undefined;
}
export interface PulsarEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly topic: string;
    readonly createTopic?: boolean | undefined;
    readonly partitions?: number | undefined;
    readonly subscriptionName?: string | undefined;
    readonly subscriptionType?: "Shared" | "Exclusive" | "Failover" | undefined;
    readonly createSubscription?: boolean | undefined;
    readonly subscriptionInitialPosition?: "Earliest" | "Latest" | undefined;
    readonly receiverQueueSize?: number | undefined;
    readonly negativeAckRedeliveryDelayMs?: number | undefined;
    readonly publisherMaxInFlight?: number | undefined;
}
export interface PulsarDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 11;
    readonly servers: readonly string[];
    readonly adminUrl: string;
    readonly adminToken?: string | undefined;
    readonly connectTimeoutMs?: number | undefined;
    readonly operationTimeoutMs?: number | undefined;
    readonly token?: string | undefined;
    readonly tlsCaFile?: string | undefined;
    readonly tlsCertFile?: string | undefined;
    readonly tlsKeyFile?: string | undefined;
}
export interface PulsarEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly topic: string;
    readonly createTopic?: boolean | undefined;
    readonly partitions?: number | undefined;
    readonly subscriptionName?: string | undefined;
    readonly subscriptionType?: "Shared" | "Exclusive" | "Failover" | undefined;
    readonly createSubscription?: boolean | undefined;
    readonly subscriptionInitialPosition?: "Earliest" | "Latest" | undefined;
    readonly receiverQueueSize?: number | undefined;
    readonly negativeAckRedeliveryDelayMs?: number | undefined;
    readonly publisherMaxInFlight?: number | undefined;
}
export interface PostgreSQLKyselyDataConnectorConfig extends DataConnectorConfig {
    readonly type: 13;
    readonly connectionString: string;
    readonly connectionsCount: number;
    readonly connectTimeoutMs: number;
    readonly operationTimeoutMs: number;
}
export interface QueryEndpointConfig extends EndpointConfig, FunctionConfig {
    readonly enabled: boolean;
    readonly queryOperation: "Rows" | "Execute" | "GetObject" | "HeadObject" | "ListObjects" | "PutObject" | "DeleteObject";
    readonly fetchSize: number;
    readonly sqlFile?: string | undefined;
    readonly bucket?: string | undefined;
}
export interface PostgreSQLKyselyDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 13 | "PostgreSQLKysely";
    readonly connectionString?: string | undefined;
    readonly connectionsCount?: number | undefined;
    readonly connectTimeoutMs?: number | undefined;
    readonly operationTimeoutMs?: number | undefined;
}
export interface QueryEndpointConfigDocument extends EndpointConfigDocumentBase {
    readonly enabled?: boolean | undefined;
    readonly queryOperation: "Rows" | "Execute" | "GetObject" | "HeadObject" | "ListObjects" | "PutObject" | "DeleteObject";
    readonly fetchSize?: number | undefined;
    readonly sqlFile?: string | undefined;
    readonly bucket?: string | undefined;
}
export interface S3AWSDataConnectorConfig extends DataConnectorConfig {
    readonly type: 18;
    readonly region: string;
    readonly address?: string | undefined;
    readonly forcePathStyle: boolean;
}
export interface S3AWSDataConnectorConfigDocument extends DataConnectorConfigDocumentBase {
    readonly type: 18 | "S3AWS";
    readonly region: string;
    readonly address?: string | undefined;
    readonly forcePathStyle?: boolean | undefined;
}
export {};
//# sourceMappingURL=types.d.ts.map