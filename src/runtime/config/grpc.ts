import type {
  DataConnectorConfig,
  EndpointConfig,
  GrpcDataConnectorConfig,
  GrpcEndpointConfig
} from "./types.js";

export function isGrpcDataConnectorConfig(
  value: DataConnectorConfig | undefined
): value is GrpcDataConnectorConfig {
  return (
    value?.type === 2 &&
    "connectionsCount" in value &&
    typeof value.connectionsCount === "number" &&
    Number.isSafeInteger(value.connectionsCount) &&
    value.connectionsCount >= 1
  );
}

export function requireGrpcDataConnectorConfig(
  value: DataConnectorConfig | undefined
): GrpcDataConnectorConfig {
  if (!isGrpcDataConnectorConfig(value)) {
    throw new Error("invalid gRPC data connector config");
  }
  return value;
}

export function isGrpcEndpointConfig(
  value: EndpointConfig | undefined
): value is GrpcEndpointConfig {
  return (
    value !== undefined &&
    "grpcMethodType" in value &&
    (value.grpcMethodType === "NoStreaming" ||
      value.grpcMethodType === "ClientStreaming" ||
      value.grpcMethodType === "ServerStreaming" ||
      value.grpcMethodType === "BidirectionalStreaming") &&
    "methodName" in value &&
    typeof value.methodName === "string"
  );
}

export function requireGrpcEndpointConfig(value: EndpointConfig | undefined): GrpcEndpointConfig {
  if (!isGrpcEndpointConfig(value)) {
    throw new Error("invalid gRPC endpoint config");
  }
  return value;
}
