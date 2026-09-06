import {
  DataConnectorType,
  type CustomDataConnectorConfig,
  type CustomEndpointConfig,
  type DataConnectorConfig,
  type EndpointConfig
} from "./types.js";

export function isCustomDataConnectorConfig(
  value: DataConnectorConfig | undefined
): value is CustomDataConnectorConfig {
  return value?.type === DataConnectorType.Custom;
}

export function requireCustomDataConnectorConfig(
  value: DataConnectorConfig | undefined
): CustomDataConnectorConfig {
  if (!isCustomDataConnectorConfig(value)) {
    throw new Error("invalid Custom data connector config");
  }
  return value;
}

export function isCustomEndpointConfig(
  value: EndpointConfig | undefined
): value is CustomEndpointConfig {
  return value !== undefined;
}

export function requireCustomEndpointConfig(
  value: EndpointConfig | undefined
): CustomEndpointConfig {
  if (!isCustomEndpointConfig(value)) {
    throw new Error("invalid Custom endpoint config");
  }
  return value;
}
