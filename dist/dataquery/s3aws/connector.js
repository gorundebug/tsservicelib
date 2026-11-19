import { S3Client } from "@aws-sdk/client-s3";
import { requireS3AWSDataConnectorConfig } from "../../runtime/config/query.js";
/** Owns the native transport; credentials use the AWS default provider chain. */
export class Connector {
    id;
    name;
    #config;
    #client;
    #clientFactory;
    /** Each factory result is owned by the connector and destroyed on stop. */
    constructor(config, clientFactory) {
        requireS3AWSDataConnectorConfig(config);
        this.#config = { ...config };
        this.#clientFactory = clientFactory;
        this.id = config.id;
        this.name = config.name;
    }
    client() {
        if (this.#client === undefined)
            throw new Error("S3 connector is not started");
        return this.#client;
    }
    async start(context) {
        context.signal().throwIfAborted();
        if (this.#client !== undefined)
            return;
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
    async startAdmission(_context) { }
    async stopAdmission(_context) { }
    async stop(_context) {
        this.#client?.destroy();
        this.#client = undefined;
    }
}
export function makeConnector(environment, config) {
    requireS3AWSDataConnectorConfig(config);
    const existing = environment.managedDataConnectorById(config.id);
    if (existing !== undefined) {
        if (!(existing instanceof Connector))
            throw new Error(`connector ${String(config.id)} already has another runtime type`);
        return existing;
    }
    const connector = new Connector(config);
    environment.addManagedDataConnector(connector);
    return connector;
}
//# sourceMappingURL=connector.js.map