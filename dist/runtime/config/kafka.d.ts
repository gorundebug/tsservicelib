import type { DataConnectorConfig, EndpointConfig, KafkaDataConnectorConfig, KafkaEndpointConfig } from "./types.js";
export declare function isKafkaDataConnectorConfig(value: DataConnectorConfig | undefined): value is KafkaDataConnectorConfig;
export declare function requireKafkaDataConnectorConfig(value: DataConnectorConfig | undefined): KafkaDataConnectorConfig;
export declare function isKafkaEndpointConfig(value: EndpointConfig | undefined): value is KafkaEndpointConfig;
export declare function requireKafkaEndpointConfig(value: EndpointConfig | undefined): KafkaEndpointConfig;
//# sourceMappingURL=kafka.d.ts.map