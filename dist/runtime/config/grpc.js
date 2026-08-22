export function isGrpcDataConnectorConfig(value) {
    return (value?.type === 2 &&
        "connectionsCount" in value &&
        typeof value.connectionsCount === "number" &&
        Number.isSafeInteger(value.connectionsCount) &&
        value.connectionsCount >= 1);
}
export function requireGrpcDataConnectorConfig(value) {
    if (!isGrpcDataConnectorConfig(value)) {
        throw new Error("invalid gRPC data connector config");
    }
    return value;
}
export function isGrpcEndpointConfig(value) {
    return (value !== undefined &&
        "grpcMethodType" in value &&
        (value.grpcMethodType === "NoStreaming" ||
            value.grpcMethodType === "ClientStreaming" ||
            value.grpcMethodType === "ServerStreaming" ||
            value.grpcMethodType === "BidirectionalStreaming") &&
        "methodName" in value &&
        typeof value.methodName === "string");
}
export function requireGrpcEndpointConfig(value) {
    if (!isGrpcEndpointConfig(value)) {
        throw new Error("invalid gRPC endpoint config");
    }
    return value;
}
//# sourceMappingURL=grpc.js.map