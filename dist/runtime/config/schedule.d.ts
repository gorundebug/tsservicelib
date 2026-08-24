import { type CronDataConnectorConfig, type CronEndpointConfig, type DataConnectorConfig, type EndpointConfig, type TemporalDataConnectorConfig, type TemporalEndpointConfig } from "./types.js";
export declare function isCronDataConnectorConfig(value: DataConnectorConfig | undefined): value is CronDataConnectorConfig;
export declare function requireCronDataConnectorConfig(value: DataConnectorConfig | undefined): CronDataConnectorConfig;
export declare function isTemporalDataConnectorConfig(value: DataConnectorConfig | undefined): value is TemporalDataConnectorConfig;
export declare function requireTemporalDataConnectorConfig(value: DataConnectorConfig | undefined): TemporalDataConnectorConfig;
export declare function isCronEndpointConfig(value: EndpointConfig | undefined): value is CronEndpointConfig;
export declare function requireCronEndpointConfig(value: EndpointConfig | undefined): CronEndpointConfig;
export declare function isTemporalEndpointConfig(value: EndpointConfig | undefined): value is TemporalEndpointConfig;
export declare function requireTemporalEndpointConfig(value: EndpointConfig | undefined): TemporalEndpointConfig;
//# sourceMappingURL=schedule.d.ts.map