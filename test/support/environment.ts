import {
  RuntimeConfig,
  RuntimeConfigStore,
  ServiceEnvironment,
  SerdeType,
  StubSerde,
  makeStreamSerde,
  type CallerFactory,
  type CanonicalConfig,
  type DataConnectorConfig,
  type DelayPool,
  type EndpointConfig,
  type JoinStorageFactory,
  type Logger,
  type Metrics,
  type PoolConfig,
  type PriorityTaskPool,
  type ServiceConfig,
  type StreamConfig,
  type StreamSerde,
  type TaskPool,
  type Tracing
} from "@gorundebug/tsservicelib/runtime";

export interface TestEnvironmentOptions {
  readonly dataConnectors?: readonly DataConnectorConfig[];
  readonly endpoints?: readonly EndpointConfig[];
  readonly callerFactory?: CallerFactory;
  readonly delayPool?: DelayPool;
  readonly logger?: Logger;
  readonly metrics?: Metrics;
  readonly tracing?: Tracing;
  readonly joinStorageFactory?: JoinStorageFactory;
  readonly service?: Partial<ServiceConfig>;
  readonly pools?: readonly PoolConfig[];
  readonly taskPools?: ReadonlyMap<string, TaskPool>;
  readonly priorityTaskPools?: ReadonlyMap<string, PriorityTaskPool>;
}

export function makeTestEnvironment(
  streams: readonly StreamConfig[],
  options: TestEnvironmentOptions = {}
): ServiceEnvironment {
  return makeTestEnvironmentWithStore(streams, options).environment;
}

export function makeTestEnvironmentWithStore(
  streams: readonly StreamConfig[],
  options: TestEnvironmentOptions = {}
): {
  readonly environment: ServiceEnvironment;
  readonly store: RuntimeConfigStore<CanonicalConfig>;
} {
  const config: CanonicalConfig = {
    services: [
      {
        id: 1,
        name: "test-service",
        color: "#000000",
        properties: {},
        environment: "test",
        grpcHost: "127.0.0.1",
        grpcPort: 9201,
        httpHost: "127.0.0.1",
        httpPort: 9091,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        ...options.service,
        startupHandler: options.service?.startupHandler ?? "/health/startup",
        readinessHandler: options.service?.readinessHandler ?? "/health/ready",
        livenessHandler: options.service?.livenessHandler ?? "/health/live",
        kubernetesWorkloadType: options.service?.kubernetesWorkloadType ?? "Deployment"
      }
    ],
    streams,
    dataConnectors: options.dataConnectors ?? [],
    endpoints: options.endpoints ?? [],
    pools: options.pools ?? [],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
  const store = new RuntimeConfigStore(new RuntimeConfig(config));
  const environment = new ServiceEnvironment(
    store,
    1,
    options.callerFactory,
    options.delayPool,
    undefined,
    options.logger,
    options.metrics,
    options.tracing,
    options.taskPools ?? new Map(),
    options.priorityTaskPools ?? new Map(),
    options.joinStorageFactory
  );
  return { environment, store };
}

export function makeTestSerde<T>(): StreamSerde<T> {
  return makeStreamSerde(new StubSerde<T>());
}

export function registerTestSerdeType<T>(
  environment: ServiceEnvironment,
  name: string,
  predicate: (value: unknown) => value is T
): SerdeType<T> {
  const type = new SerdeType(name, predicate);
  environment.serdeRegistry().register(type, makeTestSerde());
  return type;
}
