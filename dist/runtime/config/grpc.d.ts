import type { DataConnectorConfig, EndpointConfig, GrpcDataConnectorConfig, GrpcEndpointConfig } from "./types.js";
export declare function isGrpcDataConnectorConfig(value: DataConnectorConfig | undefined): value is GrpcDataConnectorConfig;
export declare function requireGrpcDataConnectorConfig(value: DataConnectorConfig | undefined): GrpcDataConnectorConfig;
export declare function isGrpcEndpointConfig(value: EndpointConfig | undefined): value is GrpcEndpointConfig;
export declare function requireGrpcEndpointConfig(value: EndpointConfig | undefined): GrpcEndpointConfig;
//# sourceMappingURL=grpc.d.ts.map