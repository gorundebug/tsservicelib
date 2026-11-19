import { S3Client } from "@aws-sdk/client-s3";
import type { Context } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import type { S3AWSDataConnectorConfig } from "../../runtime/config/types.js";
import { requireS3AWSDataConnectorConfig } from "../../runtime/config/query.js";

/** Owns the native transport; credentials use the AWS default provider chain. */
export class Connector implements ManagedDataConnector {
  public readonly id: number;
  public readonly name: string;
  readonly #config: S3AWSDataConnectorConfig;
  #client: S3Client | undefined;
  readonly #clientFactory: ((context: Context) => S3Client | Promise<S3Client>) | undefined;

  /** Each factory result is owned by the connector and destroyed on stop. */
  public constructor(config: S3AWSDataConnectorConfig, clientFactory?: (context: Context) => S3Client | Promise<S3Client>) {
    requireS3AWSDataConnectorConfig(config);
    this.#config = { ...config };
    this.#clientFactory = clientFactory;
    this.id = config.id;
    this.name = config.name;
  }
  public client(): S3Client {
    if (this.#client === undefined) throw new Error("S3 connector is not started");
    return this.#client;
  }
  public async start(context: Context): Promise<void> {
    context.signal().throwIfAborted();
    if (this.#client !== undefined) return;
    const client = this.#clientFactory !== undefined ? await this.#clientFactory(context) : new S3Client({
      region: this.#config.region,
      ...(this.#config.address ? { endpoint: this.#config.address } : {}),
      forcePathStyle: this.#config.forcePathStyle,
      maxAttempts: 1
    });
    if (context.signal().aborted) {
      client.destroy();
      context.signal().throwIfAborted();
    }
    this.#client = client;
  }
  public async startAdmission(_context: Context): Promise<void> {}
  public async stopAdmission(_context: Context): Promise<void> {}
  public async stop(_context: Context): Promise<void> {
    this.#client?.destroy();
    this.#client = undefined;
  }
}

export function makeConnector(environment: RuntimeEnvironment, config: S3AWSDataConnectorConfig): Connector {
  requireS3AWSDataConnectorConfig(config);
  const existing = environment.managedDataConnectorById(config.id);
  if (existing !== undefined) {
    if (!(existing instanceof Connector)) throw new Error(`connector ${String(config.id)} already has another runtime type`);
    return existing;
  }
  const connector = new Connector(config);
  environment.addManagedDataConnector(connector);
  return connector;
}
