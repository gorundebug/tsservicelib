import type {
  DataConnectorConfig,
  EndpointConfig,
  KafkaDataConnectorConfig,
  KafkaEndpointConfig
} from "./types.js";

export function isKafkaDataConnectorConfig(
  value: DataConnectorConfig | undefined
): value is KafkaDataConnectorConfig {
  return (
    value?.type === 3 &&
    "brokers" in value &&
    typeof value.brokers === "string" &&
    "dialTimeout" in value &&
    typeof value.dialTimeout === "number" &&
    "usePartitioner" in value &&
    typeof value.usePartitioner === "boolean" &&
    "async" in value &&
    typeof value.async === "boolean"
  );
}

export function requireKafkaDataConnectorConfig(
  value: DataConnectorConfig | undefined
): KafkaDataConnectorConfig {
  if (!isKafkaDataConnectorConfig(value)) {
    throw new Error("invalid Kafka data connector config");
  }
  return value;
}

export function isKafkaEndpointConfig(
  value: EndpointConfig | undefined
): value is KafkaEndpointConfig {
  return (
    value !== undefined &&
    "enabled" in value &&
    typeof value.enabled === "boolean" &&
    "createTopic" in value &&
    typeof value.createTopic === "boolean" &&
    "topic" in value &&
    typeof value.topic === "string" &&
    "partitions" in value &&
    typeof value.partitions === "number" &&
    "consumerGroup" in value &&
    typeof value.consumerGroup === "string" &&
    "replicationFactor" in value &&
    typeof value.replicationFactor === "number"
  );
}

export function requireKafkaEndpointConfig(value: EndpointConfig | undefined): KafkaEndpointConfig {
  if (!isKafkaEndpointConfig(value)) {
    throw new Error("invalid Kafka endpoint config");
  }
  return value;
}
