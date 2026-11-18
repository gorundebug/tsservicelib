import type { DataConnectorConfig, EndpointConfig, NATSDataConnectorConfig, NATSEndpointConfig } from "./types.js";
export declare function requireNATSDataConnectorConfig(value: DataConnectorConfig | NATSDataConnectorConfig | undefined): NATSDataConnectorConfig;
export declare function requireNATSEndpointConfig(value: EndpointConfig | undefined): NATSEndpointConfig;
export declare function validateNATSEndpoint(config: NATSEndpointConfig, kind: 7 | 8, sink: boolean): void;
//# sourceMappingURL=nats.d.ts.map