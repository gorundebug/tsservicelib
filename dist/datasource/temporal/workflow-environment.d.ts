import type { CanonicalConfig, JoinStorageType, ServiceConfig } from "../../runtime/config/types.js";
import { RuntimeConfig } from "../../runtime/config/runtime-config.js";
import type { MessageContext } from "../../runtime/context.js";
import type { DataSink } from "../../runtime/data-sink.js";
import type { DataSource } from "../../runtime/data-source.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { Logger } from "../../runtime/environment/log.js";
import type { Metrics } from "../../runtime/environment/metrics/metrics.js";
import type { RuntimeBuildable, RuntimeEnvironment, RuntimeGraphLink } from "../../runtime/environment/runtime-environment.js";
import { type Tracing } from "../../runtime/environment/tracing/index.js";
import type { PriorityTaskPoolLike, TaskPoolLike } from "../../runtime/pool/index.js";
import { type SerdeRegistry, type SerdeType } from "../../runtime/serde/registry.js";
import type { StreamSerde } from "../../runtime/serde/serde.js";
import type { ServiceHTTPServer, HTTPHandler } from "../../runtime/service-http-server.js";
import type { Storage } from "../../runtime/store/index.js";
import { type JoinStorage, type JoinStorageConfig } from "../../runtime/store/join-storage.js";
import type { Caller, Stream, TypedStreamConsumer } from "../../runtime/stream.js";
/**
 * Workflow-isolate implementation of the ordinary graph environment.
 *
 * It deliberately owns no sockets, filesystem access, SDK clients, logging,
 * metrics, or tracing exporters. Existing operators and configured call
 * semantics are reused; Temporal owns the durable execution and timer.
 */
export declare class TemporalWorkflowEnvironment implements RuntimeEnvironment {
    #private;
    constructor(config: CanonicalConfig, serviceId: number, serdeRegistry: SerdeRegistry, telemetry?: {
        readonly logger?: Logger;
        readonly metrics?: Metrics;
        readonly tracing?: Tracing;
    });
    runtimeConfig(): RuntimeConfig;
    serviceConfig(): ServiceConfig;
    registerStream(stream: Stream): void;
    registerStorage(storage: Storage): void;
    createKeyValueJoinStorage<K>(storageType: JoinStorageType, config: JoinStorageConfig, stream: Stream): JoinStorage<K>;
    storages(): readonly Storage[];
    addDataSource(dataSource: DataSource): void;
    dataSourceById(id: number): DataSource | undefined;
    dataSources(): readonly DataSource[];
    addDataSink(dataSink: DataSink): void;
    dataSinkById(id: number): DataSink | undefined;
    dataSinks(): readonly DataSink[];
    addManagedDataConnector(connector: ManagedDataConnector): void;
    managedDataConnectorById(id: number): ManagedDataConnector | undefined;
    managedDataConnectors(): readonly ManagedDataConnector[];
    log(): Logger;
    metrics(): Metrics;
    tracing(): Tracing | undefined;
    registerHttpHandler(path: string, handler: HTTPHandler): void;
    httpServer(): ServiceHTTPServer;
    registerRuntimeBuildable(buildable: RuntimeBuildable): void;
    streamById(id: number): Stream | undefined;
    runtimeStreamIds(): ReadonlySet<number>;
    graphLinks(): readonly RuntimeGraphLink[];
    runtimeStreams(): readonly Stream[];
    linkCallCount(from: number, to: number): number;
    buildRuntimeStreams(): Promise<void>;
    validateRuntimeTopology(): void;
    serdeRegistry(): SerdeRegistry;
    serde<T>(type: SerdeType<T>): StreamSerde<T>;
    serdeByName<T>(name: string): StreamSerde<T>;
    assertSerdeValue<T>(name: string, value: unknown): asserts value is T;
    streamValueSerde<T>(streamId: number): StreamSerde<T>;
    streamErrorSerde<T>(streamId: number): StreamSerde<T>;
    taskPool(name: string): TaskPoolLike | undefined;
    priorityTaskPool(name: string): PriorityTaskPoolLike | undefined;
    makeCaller<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T>;
    makeLinkRecorder(source: Stream, consumer: Stream): (context: MessageContext) => void;
    delay(context: MessageContext, delayMs: number, execute: () => void | Promise<void>): void;
    start(): Promise<void>;
    finish(): Promise<void>;
    throwIfFailed(): void;
    private makePools;
    private waitForQuiescence;
    private recordFailure;
}
//# sourceMappingURL=workflow-environment.d.ts.map