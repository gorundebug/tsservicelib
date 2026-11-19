import type { DataConnectorConfig, S3AWSDataConnectorConfig, EndpointConfig, PostgreSQLKyselyDataConnectorConfig, QueryEndpointConfig } from "./types.js";

export function requirePostgreSQLKyselyDataConnectorConfig(value: DataConnectorConfig | PostgreSQLKyselyDataConnectorConfig | undefined): PostgreSQLKyselyDataConnectorConfig {
  if (value === undefined || !("connectionString" in value) || typeof value.connectionString !== "string" ||
      !("connectionsCount" in value) || typeof value.connectionsCount !== "number" ||
      !("connectTimeoutMs" in value) || typeof value.connectTimeoutMs !== "number" ||
      !("operationTimeoutMs" in value) || typeof value.operationTimeoutMs !== "number") {
    throw new Error("invalid PostgreSQL Query connector config");
  }
  if (value.type !== 13 || value.implementation !== "node/kysely") throw new Error("PostgreSQL Query requires node/kysely");
  let valid = false;
  try {
    const url = new URL(value.connectionString);
    valid = (url.protocol === "postgres:" || url.protocol === "postgresql:") && url.pathname.length > 1;
  } catch { /* Never include the connection string in diagnostics. */ }
  if (!valid) throw new Error("PostgreSQL Query requires a PostgreSQL connection URL");
  if (!Number.isSafeInteger(value.connectionsCount) || value.connectionsCount <= 0) throw new Error("PostgreSQL connectionsCount must be positive");
  for (const timeout of [value.connectTimeoutMs, value.operationTimeoutMs]) {
    if (!Number.isSafeInteger(timeout) || timeout < 0) throw new Error("PostgreSQL timeouts must be nonnegative integers");
  }
  return value as PostgreSQLKyselyDataConnectorConfig;
}

export function requireQueryEndpointConfig(value: EndpointConfig | (Omit<QueryEndpointConfig, "queryOperation"> & { readonly queryOperation: string }) | undefined, connectorType?: 13 | 18): QueryEndpointConfig {
  if (value === undefined || !("queryOperation" in value) || !("fetchSize" in value) ||
      typeof value.fetchSize !== "number" || !("enabled" in value) || typeof value.enabled !== "boolean") {
    throw new Error("invalid Query endpoint config");
  }
  const postgresOperations = ["Rows", "Execute"];
  const s3Operations = ["GetObject", "HeadObject", "ListObjects", "PutObject", "DeleteObject"];
  const operations = connectorType === 13 ? postgresOperations : connectorType === 18 ? s3Operations : [...postgresOperations, ...s3Operations];
  if (typeof value.queryOperation !== "string" || !operations.includes(value.queryOperation)) throw new Error("Query operation is not supported by this connector");
  if (!Number.isSafeInteger(value.fetchSize) || value.fetchSize <= 0) throw new Error("Query fetchSize must be positive");
  return value as QueryEndpointConfig;
}

export function requireS3AWSDataConnectorConfig(value: DataConnectorConfig | S3AWSDataConnectorConfig | undefined): S3AWSDataConnectorConfig {
  if (value === undefined || value.type !== 18 || value.implementation !== "node/aws" ||
      !("region" in value) || typeof value.region !== "string" || !value.region.trim() ||
      !("forcePathStyle" in value) || typeof value.forcePathStyle !== "boolean") {
    throw new Error("S3 Query requires node/aws, region and forcePathStyle");
  }
  if (!Number.isSafeInteger(value.id) || value.id <= 0) throw new Error("S3 connector id must be positive");
  if ("address" in value && value.address !== undefined && value.address !== "") {
    let valid = false;
    try {
      const url = new URL(String(value.address));
      valid = typeof value.address === "string" && ["http:", "https:"].includes(url.protocol) &&
        !!url.hostname && !url.username && !url.password && !url.search && !url.hash;
    } catch { /* Endpoint diagnostics must not expose credentials. */ }
    if (!valid) throw new Error("invalid S3 endpoint address");
  }
  return value as S3AWSDataConnectorConfig;
}
