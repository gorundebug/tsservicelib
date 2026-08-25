import type {
  CanonicalConfig,
  JoinStorageType,
  ServiceConfig
} from "../../runtime/config/types.js";
import { RuntimeConfig } from "../../runtime/config/runtime-config.js";
import type { MessageContext } from "../../runtime/context.js";
import { Context } from "../../runtime/context.js";
import { RuntimeCallerFactory } from "../../runtime/caller-factory.js";
import type { DataSink } from "../../runtime/data-sink.js";
import type { DataSource } from "../../runtime/data-source.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import { noopLogger, type Logger } from "../../runtime/environment/log.js";
import { noopMetrics } from "../../runtime/environment/metrics/noop.js";
import type { Metrics } from "../../runtime/environment/metrics/metrics.js";
import type {
  RuntimeBuildable,
  RuntimeEnvironment,
  RuntimeGraphLink
} from "../../runtime/environment/runtime-environment.js";
import type { Tracing } from "../../runtime/environment/tracing/index.js";
import { PriorityTaskPool } from "../../runtime/pool/priority-task-pool.js";
import { TaskPool } from "../../runtime/pool/task-pool.js";
import { type SerdeRegistry, type SerdeType } from "../../runtime/serde/registry.js";
import type { StreamSerde } from "../../runtime/serde/serde.js";
import type { ServiceHTTPServer, HTTPHandler } from "../../runtime/service-http-server.js";
import type { Storage } from "../../runtime/store/index.js";
import {
  makeJoinStorage,
  type JoinStorage,
  type JoinStorageConfig
} from "../../runtime/store/join-storage.js";
import type { Caller, Stream, TypedStream, TypedStreamConsumer } from "../../runtime/stream.js";
import { RuntimeTaskRegistry } from "../../runtime/task-registry.js";

/**
 * Workflow-isolate implementation of the ordinary graph environment.
 *
 * It deliberately owns no sockets, filesystem access, SDK clients, logging,
 * metrics, or tracing exporters. Existing operators and configured call
 * semantics are reused; Temporal owns the durable execution and timer.
 */
export class TemporalWorkflowEnvironment implements RuntimeEnvironment {
  readonly #config: RuntimeConfig;
  readonly #serviceId: number;
  readonly #serdeRegistry: SerdeRegistry;
  readonly #streams = new Map<number, Stream>();
  readonly #dataSources = new Map<number, DataSource>();
  readonly #dataSinks = new Map<number, DataSink>();
  readonly #connectors = new Map<number, ManagedDataConnector>();
  readonly #storages = new Set<Storage>();
  readonly #buildables = new Set<RuntimeBuildable>();
  readonly #linkCallCounts = new Map<string, number>();
  readonly #tasks: RuntimeTaskRegistry;
  readonly #taskPools: ReadonlyMap<string, TaskPool>;
  readonly #priorityTaskPools: ReadonlyMap<string, PriorityTaskPool>;
  readonly #callerFactory: RuntimeCallerFactory;
  #failure: Error | undefined;
  #started = false;

  public constructor(config: CanonicalConfig, serviceId: number, serdeRegistry: SerdeRegistry) {
    this.#config = new RuntimeConfig(config);
    this.#serviceId = serviceId;
    this.#serdeRegistry = serdeRegistry;
    this.#tasks = new RuntimeTaskRegistry((error) => {
      this.recordFailure(error);
    });
    const pools = this.makePools();
    this.#taskPools = pools.task;
    this.#priorityTaskPools = pools.priority;
    this.#callerFactory = new RuntimeCallerFactory({
      config: () => this.runtimeConfig(),
      serviceId,
      taskPools: this.#taskPools,
      priorityTaskPools: this.#priorityTaskPools,
      tasks: this.#tasks,
      onRejected: (error) => {
        this.recordFailure(error);
      }
    });
  }

  public runtimeConfig(): RuntimeConfig {
    return this.#config;
  }

  public serviceConfig(): ServiceConfig {
    const config = this.runtimeConfig().serviceById(this.#serviceId);
    if (config === undefined)
      throw new Error(`service config ${String(this.#serviceId)} not found`);
    return config;
  }

  public registerStream(stream: Stream): void {
    if (this.#streams.has(stream.id))
      throw new Error(`duplicate runtime stream id ${String(stream.id)}`);
    this.#streams.set(stream.id, stream);
  }

  public registerStorage(storage: Storage): void {
    if (this.#storages.has(storage)) throw new Error("storage is already registered");
    this.#storages.add(storage);
  }

  public createKeyValueJoinStorage<K>(
    storageType: JoinStorageType,
    config: JoinStorageConfig,
    stream: Stream
  ): JoinStorage<K> {
    void stream;
    return makeJoinStorage(storageType, this, config);
  }

  public storages(): readonly Storage[] {
    return [...this.#storages];
  }

  public addDataSource(dataSource: DataSource): void {
    this.#dataSources.set(dataSource.id, dataSource);
  }

  public dataSourceById(id: number): DataSource | undefined {
    return this.#dataSources.get(id);
  }

  public dataSources(): readonly DataSource[] {
    return [...this.#dataSources.values()];
  }

  public addDataSink(dataSink: DataSink): void {
    this.#dataSinks.set(dataSink.id, dataSink);
  }

  public dataSinkById(id: number): DataSink | undefined {
    return this.#dataSinks.get(id);
  }

  public dataSinks(): readonly DataSink[] {
    return [...this.#dataSinks.values()];
  }

  public addManagedDataConnector(connector: ManagedDataConnector): void {
    const existing = this.#connectors.get(connector.id);
    if (existing !== undefined && existing !== connector) {
      throw new Error(`managed connector ${String(connector.id)} is already registered`);
    }
    this.#connectors.set(connector.id, connector);
  }

  public managedDataConnectorById(id: number): ManagedDataConnector | undefined {
    return this.#connectors.get(id);
  }

  public managedDataConnectors(): readonly ManagedDataConnector[] {
    return [...this.#connectors.values()];
  }

  public log(): Logger {
    return noopLogger;
  }

  public metrics(): Metrics {
    return noopMetrics;
  }

  public tracing(): Tracing | undefined {
    return undefined;
  }

  public registerHttpHandler(path: string, handler: HTTPHandler): void {
    void path;
    void handler;
    throw new Error("HTTP handlers are unavailable in a Temporal Workflow");
  }

  public httpServer(): ServiceHTTPServer {
    throw new Error("HTTP server is unavailable in a Temporal Workflow");
  }

  public registerRuntimeBuildable(buildable: RuntimeBuildable): void {
    if (this.#buildables.has(buildable)) throw new Error("runtime buildable is already registered");
    this.#buildables.add(buildable);
  }

  public streamById(id: number): Stream | undefined {
    return this.#streams.get(id);
  }

  public runtimeStreamIds(): ReadonlySet<number> {
    return new Set(this.#streams.keys());
  }

  public graphLinks(): readonly RuntimeGraphLink[] {
    const links: RuntimeGraphLink[] = [];
    for (const stream of this.#streams.values()) {
      if (!isTypedStream(stream)) continue;
      for (const consumer of stream.consumers()) links.push({ from: stream.id, to: consumer.id });
    }
    return links.sort((left, right) => left.from - right.from || left.to - right.to);
  }

  public runtimeStreams(): readonly Stream[] {
    return [...this.#streams.values()];
  }

  public linkCallCount(from: number, to: number): number {
    return this.#linkCallCounts.get(graphLinkKey(from, to)) ?? 0;
  }

  public async buildRuntimeStreams(): Promise<void> {
    for (const buildable of this.#buildables) await buildable.build();
  }

  public validateRuntimeTopology(): void {
    const links = new Set(this.graphLinks().map(({ from, to }) => graphLinkKey(from, to)));
    for (const config of this.runtimeConfig().config().streams) {
      if (
        config.type === "Error" ||
        (config.idService !== 0 && config.idService !== this.#serviceId)
      )
        continue;
      if (!this.#streams.has(config.id)) {
        throw new Error(`runtime stream ${config.name} (${String(config.id)}) is not registered`);
      }
      for (const source of [config.idSource, ...config.idSources]) {
        if (source !== 0 && !links.has(graphLinkKey(source, config.id))) {
          throw new Error(
            `runtime graph link from=${String(source)} to=${String(config.id)} is missing`
          );
        }
      }
    }
  }

  public serdeRegistry(): SerdeRegistry {
    return this.#serdeRegistry;
  }

  public serde<T>(type: SerdeType<T>): StreamSerde<T> {
    return this.#serdeRegistry.require(type);
  }

  public serdeByName<T>(name: string): StreamSerde<T> {
    return this.#serdeRegistry.requireByName(name);
  }

  // The generic is required by RuntimeEnvironment's assertion contract.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
  public assertSerdeValue<T>(name: string, value: unknown): asserts value is T {
    this.#serdeRegistry.assertByName<T>(name, value);
  }

  public streamValueSerde<T>(streamId: number): StreamSerde<T> {
    return this.#serdeRegistry.requireStreamValue(streamId);
  }

  public streamErrorSerde<T>(streamId: number): StreamSerde<T> {
    return this.#serdeRegistry.requireStreamError(streamId);
  }

  public taskPool(name: string): TaskPool | undefined {
    return this.#taskPools.get(name);
  }

  public priorityTaskPool(name: string): PriorityTaskPool | undefined {
    return this.#priorityTaskPools.get(name);
  }

  public makeCaller<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T> {
    const caller = this.#callerFactory.create(source, consumer);
    return {
      isAsync: () => caller.isAsync(),
      consume: (context, value) => {
        const key = graphLinkKey(source.id, consumer.id);
        this.#linkCallCounts.set(key, (this.#linkCallCounts.get(key) ?? 0) + 1);
        return caller.consume(context, value);
      }
    };
  }

  public makeLinkRecorder(source: Stream, consumer: Stream): (context: MessageContext) => void {
    const key = graphLinkKey(source.id, consumer.id);
    return (): void => {
      this.#linkCallCounts.set(key, (this.#linkCallCounts.get(key) ?? 0) + 1);
    };
  }

  public delay(
    context: MessageContext,
    delayMs: number,
    execute: () => void | Promise<void>
  ): void {
    void context;
    void delayMs;
    void execute;
    throw new Error("Temporal Workflow Delay must use its durable execution context");
  }

  public async start(): Promise<void> {
    if (this.#started) return;
    await this.buildRuntimeStreams();
    this.validateRuntimeTopology();
    const context = Context.background();
    for (const storage of this.#storages) await storage.start(context);
    for (const pool of this.#taskPools.values()) await pool.start(context);
    for (const pool of this.#priorityTaskPools.values()) await pool.start(context);
    this.#started = true;
  }

  public async finish(): Promise<void> {
    if (!this.#started) return;
    await this.waitForQuiescence();
    this.#tasks.stopAdmission();
    const context = Context.background();
    for (const pool of this.#taskPools.values()) await pool.stop(context);
    for (const pool of this.#priorityTaskPools.values()) await pool.stop(context);
    await this.#tasks.drain();
    for (const storage of this.#storages) await storage.stop(context);
    this.#started = false;
    this.throwIfFailed();
  }

  public throwIfFailed(): void {
    if (this.#failure !== undefined) throw this.#failure;
  }

  private makePools(): {
    readonly task: ReadonlyMap<string, TaskPool>;
    readonly priority: ReadonlyMap<string, PriorityTaskPool>;
  } {
    const task = new Map<string, TaskPool>();
    const priority = new Map<string, PriorityTaskPool>();
    const use = (semantics: ServiceConfig["defaultCallSemantics"]): void => {
      if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics)
        return;
      const name =
        "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
      const config = this.runtimeConfig().poolByName(name);
      if (config === undefined) throw new Error(`pool config ${name} not found`);
      if ("taskPool" in semantics) {
        if (!task.has(name)) {
          task.set(
            name,
            new TaskPool({
              name,
              executorsCount: config.executorsCount,
              onError: (error) => {
                this.recordFailure(error);
              }
            })
          );
        }
      } else if (!priority.has(name)) {
        priority.set(
          name,
          new PriorityTaskPool({
            name,
            executorsCount: config.executorsCount,
            onError: (error) => {
              this.recordFailure(error);
            }
          })
        );
      }
    };
    for (const link of this.runtimeConfig().config().links) use(link.callSemantics);
    for (const service of this.runtimeConfig().config().services) use(service.defaultCallSemantics);
    return { task, priority };
  }

  private async waitForQuiescence(): Promise<void> {
    for (;;) {
      this.throwIfFailed();
      const poolWork =
        [...this.#taskPools.values()].some(
          (pool) => pool.activeCount() > 0 || pool.queueLength() > 0
        ) ||
        [...this.#priorityTaskPools.values()].some(
          (pool) => pool.activeCount() > 0 || pool.queueLength() > 0
        );
      if (this.#tasks.activeCount() === 0 && !poolWork) {
        await Promise.resolve();
        if (
          this.#tasks.activeCount() === 0 &&
          ![...this.#taskPools.values()].some(
            (pool) => pool.activeCount() > 0 || pool.queueLength() > 0
          ) &&
          ![...this.#priorityTaskPools.values()].some(
            (pool) => pool.activeCount() > 0 || pool.queueLength() > 0
          )
        )
          return;
      }
      await Promise.resolve();
    }
  }

  private recordFailure(value: unknown): void {
    if (this.#failure !== undefined) return;
    this.#failure = value instanceof Error ? value : new Error(String(value));
  }
}

function isTypedStream<T>(stream: Stream): stream is TypedStream<T> {
  return "consumers" in stream && typeof stream.consumers === "function";
}

function graphLinkKey(from: number, to: number): string {
  return `${String(from)}:${String(to)}`;
}
