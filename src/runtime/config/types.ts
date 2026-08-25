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

export type TransformationType =
  | "Case"
  | "CycleLink"
  | "Delay"
  | "Error"
  | "Filter"
  | "FlatMap"
  | "FlatMapIterable"
  | "Input"
  | "Join"
  | "KeyBy"
  | "Map"
  | "Merge"
  | "MultiJoin"
  | "Process"
  | "Sink"
  | "Split"
  | "When";

export type TransformationName =
  | "case"
  | "cycleLink"
  | "delay"
  | "error"
  | "filter"
  | "flatMap"
  | "flatMapIterable"
  | "input"
  | "join"
  | "keyBy"
  | "map"
  | "merge"
  | "multiJoin"
  | "process"
  | "sink"
  | "split"
  | "when";

const transformationNames: Readonly<Record<TransformationType, TransformationName>> = {
  Case: "case",
  CycleLink: "cycleLink",
  Delay: "delay",
  Error: "error",
  Filter: "filter",
  FlatMap: "flatMap",
  FlatMapIterable: "flatMapIterable",
  Input: "input",
  Join: "join",
  KeyBy: "keyBy",
  Map: "map",
  Merge: "merge",
  MultiJoin: "multiJoin",
  Process: "process",
  Sink: "sink",
  Split: "split",
  When: "when"
};

export function transformationName(type: TransformationType): TransformationName {
  return transformationNames[type];
}

export interface StreamConfig extends NamedIdentity {
  readonly type: TransformationType;
  readonly pipeline: string;
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

export const JoinType = {
  Undefined: 0,
  Inner: 1,
  Left: 2,
  Right: 3,
  Outer: 4
} as const;

export type JoinType = (typeof JoinType)[keyof typeof JoinType];

export const JoinStorageType = {
  Undefined: 0,
  HashMap: 1,
  RocksDB: 2,
  Aerospike: 3
} as const;

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

export type AnyStreamConfig =
  | InputStreamConfig
  | MapStreamConfig
  | FilterStreamConfig
  | JoinStreamConfig
  | MultiJoinStreamConfig
  | ProcessStreamConfig
  | FlatMapStreamConfig
  | FlatMapIterableStreamConfig
  | KeyByStreamConfig
  | MergeStreamConfig
  | SplitStreamConfig
  | CaseStreamConfig
  | SinkStreamConfig
  | CycleLinkStreamConfig
  | DelayStreamConfig
  | WhenStreamConfig;

export interface DataConnectorConfig extends NamedIdentity {
  readonly type: DataConnectorType;
  readonly implementation: string;
}

export const DataConnectorType = {
  Undefined: 0,
  HTTP: 1,
  GRPC: 2,
  Kafka: 3,
  Custom: 4,
  Cron: 5,
  Temporal: 6
} as const;

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
  readonly maxConcurrentActivities: number;
  readonly maxConcurrentWorkflows: number;
}

export type AnyDataConnectorConfig =
  | HttpDataConnectorConfig
  | GrpcDataConnectorConfig
  | KafkaDataConnectorConfig
  | CronDataConnectorConfig
  | TemporalDataConnectorConfig
  | CustomDataConnectorConfig;

export interface EndpointConfig extends NamedIdentity {
  readonly idDataConnector: number;
  readonly tracingEnabled?: boolean | undefined;
}

export const HTTPMethodType = {
  Undefined: "",
  GET: "GET",
  POST: "POST"
} as const;

export type HTTPMethodType = (typeof HTTPMethodType)[keyof typeof HTTPMethodType];

export interface HttpEndpointConfig extends EndpointConfig, FunctionConfig {
  readonly httpMethodType: HTTPMethodType;
  readonly path: string;
}

export type GrpcMethodType =
  "NoStreaming" | "ClientStreaming" | "ServerStreaming" | "BidirectionalStreaming";

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

export type CustomEndpointConfig = EndpointConfig & FunctionConfig;

export type AnyEndpointConfig =
  | HttpEndpointConfig
  | GrpcEndpointConfig
  | KafkaEndpointConfig
  | CronEndpointConfig
  | TemporalEndpointConfig
  | CustomEndpointConfig;

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

export interface DurableCallSemanticsConfig {
  readonly idDataConnector: number;
  readonly taskQueue: string;
  readonly workflowExecutionTimeout: number;
  readonly activityStartToCloseTimeout: number;
  readonly activityHeartbeatTimeout: number;
  readonly maximumAttempts: number;
}

export type CallSemanticsGroup =
  | { readonly functionCall: FunctionCallSemanticsConfig }
  | { readonly taskPool: TaskPoolCallSemanticsConfig }
  | { readonly priorityTaskPool: PriorityTaskPoolCallSemanticsConfig }
  | { readonly parallelCall: ParallelCallSemanticsConfig }
  | { readonly durableCall: DurableCallSemanticsConfig };

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

export interface CanonicalConfig<
  TDataConnector extends DataConnectorConfig = DataConnectorConfig,
  TEndpoint extends EndpointConfig = EndpointConfig
> {
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
  readonly idService: number;
  readonly idSource?: number | undefined;
  readonly idSources?: readonly number[] | undefined;
  readonly xPos: number;
  readonly yPos: number;
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

export interface FilterStreamConfigDocument
  extends StreamConfigDocumentBase, FunctionConfigDocument {
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

export interface MultiJoinStreamConfigDocument
  extends StreamConfigDocumentBase, FunctionConfigDocument {
  readonly type: 5 | "MultiJoin";
  readonly valueType: string;
  readonly joinStorage: JoinStorageType;
  readonly ttl: number;
  readonly renewTTL: boolean;
}

export interface ProcessStreamConfigDocument
  extends StreamConfigDocumentBase, FunctionConfigDocument {
  readonly type: 6 | "Process";
  readonly pattern?: string | undefined;
}

export interface FlatMapStreamConfigDocument
  extends StreamConfigDocumentBase, FunctionConfigDocument {
  readonly type: 7 | "FlatMap";
  readonly valueType: string;
}

export interface FlatMapIterableStreamConfigDocument extends StreamConfigDocumentBase {
  readonly type: 8 | "FlatMapIterable";
  readonly valueType: string;
}

export interface KeyByStreamConfigDocument
  extends StreamConfigDocumentBase, FunctionConfigDocument {
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

export interface CycleLinkStreamConfigDocument extends StreamConfigDocumentBase {
  readonly type: 14 | "CycleLink";
}

export interface DelayStreamConfigDocument
  extends StreamConfigDocumentBase, FunctionConfigDocument {
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
  readonly maxConcurrentActivities?: number | undefined;
  readonly maxConcurrentWorkflows?: number | undefined;
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
  readonly scheduleId?: string | undefined;
  readonly workflowExecutionTimeout?: number | undefined;
  readonly activityStartToCloseTimeout?: number | undefined;
  readonly activityHeartbeatTimeout?: number | undefined;
  readonly maximumAttempts?: number | undefined;
}

export type CustomEndpointConfigDocument = EndpointConfigDocumentBase;

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
