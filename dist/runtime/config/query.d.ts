import type { DataConnectorConfig, S3AWSDataConnectorConfig, EndpointConfig, PostgreSQLKyselyDataConnectorConfig, QueryEndpointConfig } from "./types.js";
export declare function requirePostgreSQLKyselyDataConnectorConfig(value: DataConnectorConfig | PostgreSQLKyselyDataConnectorConfig | undefined): PostgreSQLKyselyDataConnectorConfig;
export declare function requireQueryEndpointConfig(value: EndpointConfig | (Omit<QueryEndpointConfig, "queryOperation"> & {
    readonly queryOperation: string;
}) | undefined, connectorType?: 13 | 18): QueryEndpointConfig;
export declare function requireS3AWSDataConnectorConfig(value: DataConnectorConfig | S3AWSDataConnectorConfig | undefined): S3AWSDataConnectorConfig;
//# sourceMappingURL=query.d.ts.map