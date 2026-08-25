import type { DataConnectorConfig, EndpointConfig } from "./config/index.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import type { AdmissionLifecycle } from "./lifecycle.js";

export interface DataConnector {
  readonly id: number;
  readonly name: string;
}

/** Shared client/worker lifecycle owned by a configured data connector. */
export interface ManagedDataConnector extends DataConnector, AdmissionLifecycle {}

export interface Endpoint {
  readonly id: number;
  readonly name: string;
  dataConnector(): DataConnector;
  config(): EndpointConfig;
  runtimeEnvironment(): RuntimeEnvironment;
}

export abstract class RuntimeDataConnector implements DataConnector {
  readonly #environment: RuntimeEnvironment;
  public readonly id: number;
  public readonly name: string;

  protected constructor(connectorId: number, environment: RuntimeEnvironment) {
    const config = environment.runtimeConfig().dataConnectorById(connectorId);
    if (config === undefined) {
      throw new Error(`data connector config ${String(connectorId)} not found`);
    }
    this.id = connectorId;
    this.name = config.name;
    this.#environment = environment;
  }

  public config(): DataConnectorConfig {
    const config = this.#environment.runtimeConfig().dataConnectorById(this.id);
    if (config === undefined) {
      throw new Error(`data connector config ${String(this.id)} not found`);
    }
    return config;
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    return this.#environment;
  }
}
