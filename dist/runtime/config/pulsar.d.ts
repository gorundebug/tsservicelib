import type { DataConnectorConfig, EndpointConfig, PulsarDataConnectorConfig, PulsarEndpointConfig } from "./types.js";
export declare function requirePulsarDataConnectorConfig(value: DataConnectorConfig | PulsarDataConnectorConfig | undefined): PulsarDataConnectorConfig;
export declare function requirePulsarEndpointConfig(value: EndpointConfig | PulsarEndpointConfig | undefined): PulsarEndpointConfig;
export declare function validatePulsarEndpoint(c: PulsarEndpointConfig, sink: boolean): void;
//# sourceMappingURL=pulsar.d.ts.map