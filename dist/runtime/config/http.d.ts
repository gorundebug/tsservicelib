import type { DataConnectorConfig, EndpointConfig, HttpDataConnectorConfig, HttpEndpointConfig } from "./types.js";
export declare function isHttpDataConnectorConfig(value: DataConnectorConfig | undefined): value is HttpDataConnectorConfig;
export declare function requireHttpDataConnectorConfig(value: DataConnectorConfig | undefined): HttpDataConnectorConfig;
export declare function isHttpEndpointConfig(value: EndpointConfig | undefined): value is HttpEndpointConfig;
export declare function requireHttpEndpointConfig(value: EndpointConfig | undefined): HttpEndpointConfig;
//# sourceMappingURL=http.d.ts.map