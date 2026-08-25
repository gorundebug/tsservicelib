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
  TemporalEndpointConfig,
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
      if (connector.type === 6) {
        if (!("address" in connector) || connector.address === "") {
          throw new Error(`Temporal data connector ${connector.name} requires address`);
        }
        if (!("namespace" in connector) || connector.namespace === "") {
          throw new Error(`Temporal data connector ${connector.name} requires namespace`);
        }
        const cert = "tlsCertFile" in connector ? connector.tlsCertFile : "";
        const key = "tlsKeyFile" in connector ? connector.tlsKeyFile : "";
        const ca = "tlsCaFile" in connector ? connector.tlsCaFile : "";
        const serverName = "tlsServerName" in connector ? connector.tlsServerName : "";
        if ((cert === "") !== (key === "")) {
          throw new Error(
            `Temporal data connector ${connector.name} requires both TLS cert and key`
          );
        }
        if (
          "tlsEnabled" in connector &&
          !connector.tlsEnabled &&
          (cert !== "" || key !== "" || ca !== "" || serverName !== "")
        ) {
          throw new Error(
            `Temporal data connector ${connector.name} cannot configure TLS files while TLS is disabled`
          );
        }
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
      if (isTemporalEndpoint(endpoint)) {
        if (endpoint.taskQueue === "") {
          throw new Error(`Temporal endpoint ${endpoint.name} requires taskQueue`);
        }
        if (endpoint.activityStartToCloseTimeout < 1) {
          throw new Error(
            `Temporal endpoint ${endpoint.name} requires activityStartToCloseTimeout`
          );
        }
        if (endpoint.maximumAttempts < 1) {
          throw new Error(`Temporal endpoint ${endpoint.name} requires maximumAttempts`);
        }
        if (endpoint.schedule !== "" && (endpoint.scheduleId === "" || endpoint.timezone === "")) {
          throw new Error(
            `scheduled Temporal endpoint ${endpoint.name} requires scheduleId, timezone and policies`
          );
        }
        if (
          endpoint.schedule === "" &&
          (endpoint.scheduleId !== "" || endpoint.timezone !== "UTC")
        ) {
          throw new Error(
            `on-demand Temporal endpoint ${endpoint.name} cannot configure scheduleId or timezone`
          );
        }
      }
      if ("schedule" in endpoint && "timezone" in endpoint && endpoint.timezone !== "UTC") {
        throw new Error(`scheduled endpoint ${endpoint.name} requires timezone UTC`);
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
    const poolName =
      "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
    if (poolName !== "" && !this.#pools.has(poolName)) {
      throw new Error(`${owner} references missing pool ${poolName}`);
    }
  }
}

function isTemporalEndpoint(endpoint: EndpointConfig): endpoint is TemporalEndpointConfig {
  return "taskQueue" in endpoint && typeof endpoint.taskQueue === "string";
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
