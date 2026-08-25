import type { CanonicalConfig, JoinStorageType, ServiceConfig } from "../config/index.js";
import type { RuntimeConfig, RuntimeConfigStore } from "../config/index.js";
import type { MessageContext } from "../context.js";
import { callerMetadata } from "../caller-metadata.js";
import type { DataSink } from "../data-sink.js";
import type { DataSource } from "../data-source.js";
import {
  DurableDelayCaller,
  type DurableContinuationHandler,
  type DurableTransport
} from "../durable.js";
import type { DurableContinuation } from "../durable-call-context.js";
import { DelayPool, type PriorityTaskPool, type TaskPool } from "../pool/index.js";
import type { JoinStorage, JoinStorageConfig, Storage } from "../store/index.js";
import { ServiceHTTPServer, type HTTPHandler } from "../service-http-server.js";
import {
  makeDefaultSerdeRegistry,
  type SerdeRegistry,
  type SerdeType,
  type StreamSerde
} from "../serde/index.js";
import {
  FunctionCaller,
  type Caller,
  type CallerFactory,
  type Stream,
  type TypedStream,
  type TypedStreamConsumer
} from "../stream.js";
import { noopLogger, type Logger } from "./log.js";
import { noopMetrics, type Metrics } from "./metrics/index.js";
import type { Tracing } from "./tracing/index.js";
import { stringAttribute, type Attribute, type Tracer } from "./tracing/index.js";

export interface RuntimeGraphLink {
  readonly from: number;
  readonly to: number;
}

export type JoinStorageFactory = <K>(
  storageType: JoinStorageType,
  config: JoinStorageConfig,
  stream: Stream
) => JoinStorage<K> | undefined;

export interface RuntimeEnvironment {
  runtimeConfig(): RuntimeConfig;
  serviceConfig(): ServiceConfig;
  registerStream(stream: Stream): void;
  registerStorage(storage: Storage): void;
  createKeyValueJoinStorage<K>(
    storageType: JoinStorageType,
    config: JoinStorageConfig,
    stream: Stream
  ): JoinStorage<K> | undefined;
  storages(): readonly Storage[];
  addDataSource(dataSource: DataSource): void;
  dataSourceById(id: number): DataSource | undefined;
  dataSources(): readonly DataSource[];
  addDataSink(dataSink: DataSink): void;
  dataSinkById(id: number): DataSink | undefined;
  dataSinks(): readonly DataSink[];
  addDurableTransport(transport: DurableTransport): void;
  durableTransportById(id: number): DurableTransport | undefined;
  durableTransports(): readonly DurableTransport[];
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
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
  assertSerdeValue<T>(name: string, value: unknown): asserts value is T;
  streamValueSerde<T>(streamId: number): StreamSerde<T>;
  streamErrorSerde<T>(streamId: number): StreamSerde<T>;
  taskPool(name: string): TaskPool | undefined;
  priorityTaskPool(name: string): PriorityTaskPool | undefined;
  makeCaller<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T>;
  registerDurableContinuation(
    fromName: string,
    toName: string,
    handler: DurableContinuationHandler
  ): void;
  resumeDurableContinuation(
    context: MessageContext,
    continuation: DurableContinuation
  ): Promise<void>;
  makeLinkRecorder(source: Stream, consumer: Stream): (context: MessageContext) => void;
  delay(context: MessageContext, delayMs: number, execute: () => void | Promise<void>): void;
}

export interface RuntimeBuildable {
  build(): void | Promise<void>;
}

class DirectCallerFactory implements CallerFactory {
  public create<T>(_source: Stream, consumer: TypedStreamConsumer<T>): Caller<T> {
    return new FunctionCaller(consumer);
  }
}

export class ServiceEnvironment<
  T extends CanonicalConfig = CanonicalConfig
> implements RuntimeEnvironment {
  readonly #config: RuntimeConfigStore<T>;
  readonly #serviceId: number;
  readonly #callerFactory: CallerFactory;
  readonly #streams = new Map<number, Stream>();
  readonly #dataSources = new Map<number, DataSource>();
  readonly #dataSinks = new Map<number, DataSink>();
  readonly #durableTransports = new Map<number, DurableTransport>();
  readonly #durableContinuations = new Map<string, DurableContinuationHandler>();
  readonly #storages = new Set<Storage>();
  readonly #buildables = new Set<RuntimeBuildable>();
  readonly #logger: Logger;
  readonly #metrics: Metrics;
  readonly #tracing: Tracing | undefined;
  readonly #httpServer: ServiceHTTPServer;
  readonly #linkCallCounts = new Map<string, LinkCallStatistics>();
  readonly #taskPools: ReadonlyMap<string, TaskPool>;
  readonly #priorityTaskPools: ReadonlyMap<string, PriorityTaskPool>;
  readonly #joinStorageFactory: JoinStorageFactory | undefined;

  public constructor(
    config: RuntimeConfigStore<T>,
    serviceId: number,
    callerFactory: CallerFactory = new DirectCallerFactory(),
    delayPool: DelayPool = new DelayPool(),
    serdeRegistry: SerdeRegistry = makeDefaultSerdeRegistry(),
    logger: Logger = noopLogger,
    metrics: Metrics = noopMetrics,
    tracing?: Tracing,
    taskPools: ReadonlyMap<string, TaskPool> = new Map(),
    priorityTaskPools: ReadonlyMap<string, PriorityTaskPool> = new Map(),
    joinStorageFactory?: JoinStorageFactory
  ) {
    this.#config = config;
    this.#serviceId = serviceId;
    this.#callerFactory = callerFactory;
    this.#delayPool = delayPool;
    this.#serdeRegistry = serdeRegistry;
    this.#logger = logger;
    this.#metrics = metrics;
    this.#tracing = tracing?.enabled() === true ? tracing : undefined;
    this.#taskPools = taskPools;
    this.#priorityTaskPools = priorityTaskPools;
    this.#joinStorageFactory = joinStorageFactory;
    this.#httpServer = new ServiceHTTPServer(() => this.serviceConfig());
    if (metrics.enabled()) {
      const service = this.serviceConfig();
      const serviceScope = metrics.scope("service", { service: service.name });
      serviceScope
        .gauge("info", "Service information (value is always 1)", {
          environment: service.environment
        })
        .set(1);
      serviceScope.counter("config_reloads_total", "Total number of config reload attempts", {
        event: "success"
      });
      serviceScope.counter("config_reloads_total", "Total number of config reload attempts", {
        event: "error"
      });
    }
  }

  public runtimeConfig(): RuntimeConfig<T> {
    return this.#config.current();
  }

  public taskPool(name: string): TaskPool | undefined {
    return this.#taskPools.get(name);
  }

  public priorityTaskPool(name: string): PriorityTaskPool | undefined {
    return this.#priorityTaskPools.get(name);
  }

  public serviceConfig(): ServiceConfig {
    const config = this.runtimeConfig().serviceById(this.#serviceId);
    if (config === undefined) {
      throw new Error(`service config ${String(this.#serviceId)} not found`);
    }
    return config;
  }

  public registerStream(stream: Stream): void {
    if (this.#streams.has(stream.id)) {
      throw new Error(`duplicate runtime stream id ${String(stream.id)}`);
    }
    this.#streams.set(stream.id, stream);
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
      if (!isTypedStream(stream)) {
        continue;
      }
      for (const consumer of stream.consumers()) {
        links.push({ from: stream.id, to: consumer.id });
      }
    }
    return links.sort((left, right) => left.from - right.from || left.to - right.to);
  }

  public runtimeStreams(): readonly Stream[] {
    return [...this.#streams.values()];
  }

  public linkCallCount(from: number, to: number): number {
    return this.#linkCallCounts.get(graphLinkKey(from, to))?.count ?? 0;
  }

  public async buildRuntimeStreams(): Promise<void> {
    for (const buildable of this.#buildables) {
      await buildable.build();
    }
  }

  public validateRuntimeTopology(): void {
    const runtime = this.runtimeConfig();
    const links = new Set(this.graphLinks().map(({ from, to }) => graphLinkKey(from, to)));
    for (const config of runtime.config().streams) {
      if (
        config.type === "Error" ||
        (config.idService !== 0 && config.idService !== this.#serviceId)
      ) {
        continue;
      }
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

  public registerStorage(storage: Storage): void {
    if (this.#storages.has(storage)) {
      throw new Error("storage is already registered");
    }
    this.#storages.add(storage);
  }

  public createKeyValueJoinStorage<K>(
    storageType: JoinStorageType,
    config: JoinStorageConfig,
    stream: Stream
  ): JoinStorage<K> | undefined {
    return this.#joinStorageFactory?.<K>(storageType, config, stream);
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

  public addDurableTransport(transport: DurableTransport): void {
    const existing = this.#durableTransports.get(transport.id);
    if (existing !== undefined && existing !== transport) {
      throw new Error(`durable transport ${String(transport.id)} is already registered`);
    }
    this.#durableTransports.set(transport.id, transport);
  }

  public durableTransportById(id: number): DurableTransport | undefined {
    return this.#durableTransports.get(id);
  }

  public durableTransports(): readonly DurableTransport[] {
    return [...this.#durableTransports.values()];
  }

  public log(): Logger {
    return this.#logger;
  }

  public metrics(): Metrics {
    return this.#metrics;
  }

  public tracing(): Tracing | undefined {
    return this.#tracing;
  }

  public registerHttpHandler(path: string, handler: HTTPHandler): void {
    this.#httpServer.register(path, handler);
  }

  public httpServer(): ServiceHTTPServer {
    return this.#httpServer;
  }

  public storages(): readonly Storage[] {
    return [...this.#storages];
  }

  public registerRuntimeBuildable(buildable: RuntimeBuildable): void {
    if (this.#buildables.has(buildable)) {
      throw new Error("runtime buildable is already registered");
    }
    this.#buildables.add(buildable);
  }

  public buildables(): readonly RuntimeBuildable[] {
    return [...this.#buildables];
  }

  public makeCaller<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T> {
    const caller = this.#callerFactory.create(source, consumer);
    const metadata = callerMetadata(caller);
    const traceAttributes =
      this.#tracing === undefined
        ? undefined
        : [
            stringAttribute("from", source.name),
            stringAttribute("to", consumer.name),
            ...(metadata === undefined ? [] : [stringAttribute("type", metadata.type)]),
            ...(metadata?.taskPoolName === undefined
              ? []
              : [stringAttribute("taskpoolname", metadata.taskPoolName)])
          ];
    const instrumented = new InstrumentedCaller(
      caller,
      this.makeLinkRecorder(source, consumer),
      this.#tracing?.tracer(this.serviceConfig().name),
      traceAttributes
    );
    if (
      this.runtimeConfig().streamById(source.id)?.type !== "Delay" ||
      !isTypedStream<T>(source)
    ) {
      return instrumented;
    }
    this.registerDurableContinuation(source.name, consumer.name, async (context, continuation) => {
      await instrumented.consume(context, source.serde().deserialize(continuation.payload));
    });
    return new DurableDelayCaller(instrumented, source.name, consumer.name, source.serde());
  }

  public registerDurableContinuation(
    fromName: string,
    toName: string,
    handler: DurableContinuationHandler
  ): void {
    const key = durableContinuationKey(fromName, toName);
    if (this.#durableContinuations.has(key)) {
      throw new Error(`durable continuation ${fromName}->${toName} is already registered`);
    }
    this.#durableContinuations.set(key, handler);
  }

  public async resumeDurableContinuation(
    context: MessageContext,
    continuation: DurableContinuation
  ): Promise<void> {
    if (continuation.fromName === "" || continuation.toName === "" || continuation.callId === "") {
      throw new Error("invalid durable continuation envelope");
    }
    const handler = this.#durableContinuations.get(
      durableContinuationKey(continuation.fromName, continuation.toName)
    );
    if (handler === undefined) {
      throw new Error(
        `durable continuation ${continuation.fromName}->${continuation.toName} is not registered`
      );
    }
    await handler(context, continuation);
  }

  public makeLinkRecorder(source: Stream, consumer: Stream): (context: MessageContext) => void {
    const key = graphLinkKey(source.id, consumer.id);
    const statistics: LinkCallStatistics = { count: 0 };
    this.#linkCallCounts.set(key, statistics);
    const counter = this.#metrics.enabled()
      ? this.#metrics
          .scope("stream", { service: this.serviceConfig().name })
          .counter("messages_total", "Total number of messages processed by stream link", {
            from: source.name,
            to: consumer.name
          })
      : undefined;
    return (context: MessageContext): void => {
      statistics.count += 1;
      counter?.inc(context);
    };
  }

  readonly #delayPool: DelayPool;
  readonly #serdeRegistry: SerdeRegistry;

  public serdeRegistry(): SerdeRegistry {
    return this.#serdeRegistry;
  }

  public serde<T>(type: SerdeType<T>): StreamSerde<T> {
    return this.#serdeRegistry.require(type);
  }

  public serdeByName<T>(name: string): StreamSerde<T> {
    return this.#serdeRegistry.requireByName(name);
  }

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
  public assertSerdeValue<T>(name: string, value: unknown): asserts value is T {
    this.#serdeRegistry.assertByName<T>(name, value);
  }

  public streamErrorSerde<T>(streamId: number): StreamSerde<T> {
    return this.#serdeRegistry.requireStreamError(streamId);
  }

  public streamValueSerde<T>(streamId: number): StreamSerde<T> {
    return this.#serdeRegistry.requireStreamValue(streamId);
  }

  public delay(
    context: MessageContext,
    delayMs: number,
    execute: () => void | Promise<void>
  ): void {
    this.#delayPool.delay(context, delayMs, execute);
  }

  public delayPool(): DelayPool {
    return this.#delayPool;
  }
}

class InstrumentedCaller<T> implements Caller<T> {
  public constructor(
    private readonly caller: Caller<T>,
    private readonly recordCall: (context: MessageContext) => void,
    private readonly tracer: Tracer | undefined,
    private readonly traceAttributes: readonly Attribute[] | undefined
  ) {}

  public isAsync(): boolean {
    return this.caller.isAsync();
  }

  public consume(context: MessageContext, value: T): void | Promise<void> {
    this.recordCall(context);
    if (this.tracer === undefined || !context.samplingEnabled()) {
      return this.caller.consume(context, value);
    }
    const started = this.tracer.start(context, "stream.call", this.traceAttributes);
    let completion: void | Promise<void>;
    try {
      completion = this.caller.consume(started.context, value);
    } catch (error: unknown) {
      started.span.end();
      throw error;
    }
    if (completion === undefined) {
      started.span.end();
      return;
    }
    return completion.finally(() => {
      started.span.end();
    });
  }
}

function isTypedStream<T>(stream: Stream): stream is TypedStream<T> {
  return "consumers" in stream && typeof stream.consumers === "function";
}

function durableContinuationKey(fromName: string, toName: string): string {
  return `${fromName}\0${toName}`;
}

function graphLinkKey(from: number, to: number): string {
  return `${String(from)}:${String(to)}`;
}

interface LinkCallStatistics {
  count: number;
}
