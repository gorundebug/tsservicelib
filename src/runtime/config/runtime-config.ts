import type {
  CallSemanticsGroup,
  CanonicalConfig,
  DataConnectorConfig,
  EndpointConfig,
  LinkConfig,
  ModuleConfig,
  NamedIdentity,
  PoolConfig,
  ServiceConfig,
  StreamConfig,
  TypeConfig
} from "./types.js";
import { deepFreeze } from "./immutable.js";

function indexIdentities<T extends NamedIdentity>(
  category: string,
  values: readonly T[]
): { readonly byId: ReadonlyMap<number, T>; readonly byName: ReadonlyMap<string, T> } {
  const byId = new Map<number, T>();
  const byName = new Map<string, T>();
  for (const value of values) {
    if (byName.has(value.name)) {
      throw new Error(`duplicate ${category} name: ${value.name}`);
    }
    if (byId.has(value.id)) {
      throw new Error(`duplicate ${category} id: ${String(value.id)}`);
    }
    byName.set(value.name, value);
    byId.set(value.id, value);
  }
  return { byId, byName };
}

function linkKey(from: number, to: number): string {
  return `${String(from)}:${String(to)}`;
}

export class RuntimeConfig<T extends CanonicalConfig = CanonicalConfig> {
  readonly #config: T;
  readonly #services: ReturnType<typeof indexIdentities<ServiceConfig>>;
  readonly #streams: ReturnType<typeof indexIdentities<StreamConfig>>;
  readonly #connectors: ReturnType<typeof indexIdentities<DataConnectorConfig>>;
  readonly #endpoints: ReturnType<typeof indexIdentities<EndpointConfig>>;
  readonly #pools = new Map<string, PoolConfig>();
  readonly #modules = new Map<string, ModuleConfig>();
  readonly #types = new Map<string, TypeConfig>();
  readonly #links = new Map<string, LinkConfig>();

  public constructor(config: T) {
    this.#config = deepFreeze(config);
    this.#services = indexIdentities("service", this.#config.services);
    this.#streams = indexIdentities("stream", this.#config.streams);
    this.#connectors = indexIdentities("data connector", this.#config.dataConnectors);
    this.#endpoints = indexIdentities("endpoint", this.#config.endpoints);
    this.indexNamed("pool", this.#config.pools, this.#pools);
    this.indexNamed("module", this.#config.modules, this.#modules);
    this.indexNamed("type", this.#config.types, this.#types);
    this.indexLinks(this.#config.links);
    this.validateReferences();
  }

  public config(): T {
    return this.#config;
  }

  public serviceByName(name: string): ServiceConfig | undefined {
    return this.#services.byName.get(name);
  }

  public serviceById(id: number): ServiceConfig | undefined {
    return this.#services.byId.get(id);
  }

  public streamByName(name: string): StreamConfig | undefined {
    return this.#streams.byName.get(name);
  }

  public streamById(id: number): StreamConfig | undefined {
    return this.#streams.byId.get(id);
  }

  public dataConnectorById(id: number): DataConnectorConfig | undefined {
    return this.#connectors.byId.get(id);
  }

  public endpointById(id: number): EndpointConfig | undefined {
    return this.#endpoints.byId.get(id);
  }

  public poolByName(name: string): PoolConfig | undefined {
    return this.#pools.get(name);
  }

  public moduleByName(name: string): ModuleConfig | undefined {
    return this.#modules.get(name);
  }

  public typeByName(name: string): TypeConfig | undefined {
    return this.#types.get(name);
  }

  public link(from: number, to: number): LinkConfig | undefined {
    return this.#links.get(linkKey(from, to));
  }

  private indexNamed<TValue extends { readonly name: string }>(
    category: string,
    values: readonly TValue[],
    target: Map<string, TValue>
  ): void {
    for (const value of values) {
      if (target.has(value.name)) {
        throw new Error(`duplicate ${category} name: ${value.name}`);
      }
      target.set(value.name, value);
    }
  }

  private indexLinks(links: readonly LinkConfig[]): void {
    for (const link of links) {
      const key = linkKey(link.from, link.to);
      if (this.#links.has(key)) {
        throw new Error(`duplicate link from=${String(link.from)} to=${String(link.to)}`);
      }
      this.#links.set(key, link);
    }
  }

  private validateReferences(): void {
    for (const service of this.#config.services) {
      validatePort(service.grpcPort, `service ${service.name} grpcPort`);
      validatePort(service.httpPort, `service ${service.name} httpPort`);
      validateNonNegative(service.shutdownTimeout, `service ${service.name} shutdownTimeout`);
      if (service.defaultGrpcTimeout !== undefined) {
        validateNonNegative(
          service.defaultGrpcTimeout,
          `service ${service.name} defaultGrpcTimeout`
        );
      }
      this.validateCallSemantics(service.defaultCallSemantics, `service ${service.name}`);
    }
    for (const pool of this.#config.pools) {
      validatePositive(pool.executorsCount, `pool ${pool.name} executorsCount`);
      validateNonNegative(pool.queueCapacity, `pool ${pool.name} queueCapacity`);
    }
    for (const connector of this.#config.dataConnectors) {
      if (connector.type === 2) {
        const connectionsCount =
          "connectionsCount" in connector ? connector.connectionsCount : undefined;
        if (typeof connectionsCount !== "number") {
          throw new RangeError(
            `gRPC data connector ${connector.name} connectionsCount must be a positive integer`
          );
        }
        validatePositive(
          connectionsCount,
          `gRPC data connector ${connector.name} connectionsCount`
        );
      }
    }
    for (const stream of this.#config.streams) {
      if (!this.#services.byId.has(stream.idService)) {
        throw new Error(
          `stream ${stream.name} references missing service id ${String(stream.idService)}`
        );
      }
      for (const sourceId of [stream.idSource, ...stream.idSources]) {
        if (sourceId !== 0 && !this.#streams.byId.has(sourceId)) {
          throw new Error(
            `stream ${stream.name} references missing source stream id ${String(sourceId)}`
          );
        }
      }
      if ((stream.type === "Input" || stream.type === "Sink") && "idEndpoint" in stream) {
        const endpointId = stream.idEndpoint;
        if (typeof endpointId !== "number" || !this.#endpoints.byId.has(endpointId)) {
          throw new Error(
            `${stream.type} stream ${stream.name} references missing endpoint id ${String(endpointId)}`
          );
        }
      }
    }
    for (const endpoint of this.#config.endpoints) {
      const connector = this.#connectors.byId.get(endpoint.idDataConnector);
      if (connector === undefined) {
        throw new Error(
          `endpoint ${endpoint.name} references missing data connector id ${String(endpoint.idDataConnector)}`
        );
      }
      const endpointType =
        "httpMethodType" in endpoint
          ? 1
          : "grpcMethodType" in endpoint
            ? 2
            : "topic" in endpoint
              ? 3
              : "taskQueue" in endpoint
                ? 6
                : "schedule" in endpoint
                  ? 5
                  : 4;
      if (connector.type !== endpointType) {
        throw new Error(
          `endpoint ${endpoint.name} type does not match data connector ${connector.name}`
        );
      }
    }
    for (const link of this.#config.links) {
      if (!this.#streams.byId.has(link.from) || !this.#streams.byId.has(link.to)) {
        throw new Error(
          `link from=${String(link.from)} to=${String(link.to)} references missing stream`
        );
      }
      this.validateCallSemantics(
        link.callSemantics,
        `link from=${String(link.from)} to=${String(link.to)}`
      );
    }
  }

  private validateCallSemantics(semantics: CallSemanticsGroup | undefined, owner: string): void {
    if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics) {
      return;
    }
    if ("durableCall" in semantics) {
      const connector = this.#connectors.byId.get(semantics.durableCall.idDataConnector);
      if (connector === undefined) {
        throw new Error(
          `${owner} references missing Temporal data connector ${String(semantics.durableCall.idDataConnector)}`
        );
      }
      if (connector.type !== 6) {
        throw new Error(`${owner} requires a Temporal data connector`);
      }
      return;
    }
    const poolName =
      "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
    if (poolName !== "" && !this.#pools.has(poolName)) {
      throw new Error(`${owner} references missing pool ${poolName}`);
    }
  }
}

function validatePort(value: number, path: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 65_535) {
    throw new RangeError(`${path} must be an integer between 0 and 65535`);
  }
}

function validateNonNegative(value: number, path: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${path} must be a non-negative integer`);
  }
}

function validatePositive(value: number, path: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${path} must be a positive integer`);
  }
}
