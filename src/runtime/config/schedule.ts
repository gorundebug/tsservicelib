import {
  DataConnectorType,
  type CronDataConnectorConfig,
  type CronEndpointConfig,
  type DataConnectorConfig,
  type EndpointConfig,
  type TemporalDataConnectorConfig,
  type TemporalEndpointConfig
} from "./types.js";

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isPositiveInteger(value: unknown): value is number {
  return isNonNegativeInteger(value) && value > 0;
}

export function isCronDataConnectorConfig(
  value: DataConnectorConfig | undefined
): value is CronDataConnectorConfig {
  return value?.type === DataConnectorType.Cron;
}

export function requireCronDataConnectorConfig(
  value: DataConnectorConfig | undefined
): CronDataConnectorConfig {
  if (!isCronDataConnectorConfig(value)) {
    throw new Error("invalid Cron data connector config");
  }
  return value;
}

export function isTemporalDataConnectorConfig(
  value: DataConnectorConfig | undefined
): value is TemporalDataConnectorConfig {
  return (
    value?.type === DataConnectorType.Temporal &&
    "address" in value &&
    typeof value.address === "string" &&
    value.address.length > 0 &&
    "namespace" in value &&
    typeof value.namespace === "string" &&
    value.namespace.length > 0 &&
    "identity" in value &&
    typeof value.identity === "string" &&
    "apiKey" in value &&
    typeof value.apiKey === "string" &&
    "tlsEnabled" in value &&
    typeof value.tlsEnabled === "boolean" &&
    "tlsServerName" in value &&
    typeof value.tlsServerName === "string" &&
    "tlsCaFile" in value &&
    typeof value.tlsCaFile === "string" &&
    "tlsCertFile" in value &&
    typeof value.tlsCertFile === "string" &&
    "tlsKeyFile" in value &&
    typeof value.tlsKeyFile === "string" &&
    "maxConcurrentActivities" in value &&
    isPositiveInteger(value.maxConcurrentActivities) &&
    "maxConcurrentWorkflows" in value &&
    isPositiveInteger(value.maxConcurrentWorkflows)
  );
}

export function requireTemporalDataConnectorConfig(
  value: DataConnectorConfig | undefined
): TemporalDataConnectorConfig {
  if (!isTemporalDataConnectorConfig(value)) {
    throw new Error("invalid Temporal data connector config");
  }
  return value;
}

export function isCronEndpointConfig(
  value: EndpointConfig | undefined
): value is CronEndpointConfig {
  return (
    value !== undefined &&
    "enabled" in value &&
    typeof value.enabled === "boolean" &&
    "schedule" in value &&
    typeof value.schedule === "string" &&
    "timezone" in value &&
    value.timezone === "UTC" &&
    "overlapPolicy" in value &&
    (value.overlapPolicy === "Allow" || value.overlapPolicy === "Skip") &&
    "missedRunPolicy" in value &&
    (value.missedRunPolicy === "FireOnce" || value.missedRunPolicy === "Skip")
  );
}

export function requireCronEndpointConfig(value: EndpointConfig | undefined): CronEndpointConfig {
  if (!isCronEndpointConfig(value)) {
    throw new Error("invalid Cron endpoint config");
  }
  return value;
}

export function isTemporalEndpointConfig(
  value: EndpointConfig | undefined
): value is TemporalEndpointConfig {
  return (
    isCronEndpointConfig(value) &&
    "taskQueue" in value &&
    typeof value.taskQueue === "string" &&
    value.taskQueue.length > 0 &&
    "scheduleId" in value &&
    typeof value.scheduleId === "string" &&
    "workflowExecutionTimeout" in value &&
    isNonNegativeInteger(value.workflowExecutionTimeout) &&
    "activityStartToCloseTimeout" in value &&
    isPositiveInteger(value.activityStartToCloseTimeout) &&
    "activityHeartbeatTimeout" in value &&
    isNonNegativeInteger(value.activityHeartbeatTimeout) &&
    "maximumAttempts" in value &&
    isPositiveInteger(value.maximumAttempts)
  );
}

export function requireTemporalEndpointConfig(
  value: EndpointConfig | undefined
): TemporalEndpointConfig {
  if (!isTemporalEndpointConfig(value)) {
    throw new Error("invalid Temporal endpoint config");
  }
  return value;
}
