import { S3Client } from "@aws-sdk/client-s3";
import type { Context } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import type { S3AWSDataConnectorConfig } from "../../runtime/config/types.js";
/** Owns the native transport; credentials use the AWS default provider chain. */
export declare class Connector implements ManagedDataConnector {
    #private;
    readonly id: number;
    readonly name: string;
    /** Each factory result is owned by the connector and destroyed on stop. */
    constructor(config: S3AWSDataConnectorConfig, clientFactory?: (context: Context) => S3Client | Promise<S3Client>);
    client(): S3Client;
    start(context: Context): Promise<void>;
    startAdmission(_context: Context): Promise<void>;
    stopAdmission(_context: Context): Promise<void>;
    stop(_context: Context): Promise<void>;
}
export declare function makeConnector(environment: RuntimeEnvironment, config: S3AWSDataConnectorConfig): Connector;
//# sourceMappingURL=connector.d.ts.map