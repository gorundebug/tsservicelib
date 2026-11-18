import type { DataConnectorConfig, EndpointConfig, RedisStreamsDataConnectorConfig, RedisStreamsEndpointConfig } from "./types.js";
export declare function requireRedisStreamsDataConnectorConfig(value: DataConnectorConfig | RedisStreamsDataConnectorConfig | undefined): RedisStreamsDataConnectorConfig;
export declare function requireRedisStreamsEndpointConfig(value: EndpointConfig | RedisStreamsEndpointConfig | undefined): RedisStreamsEndpointConfig;
export declare function validateRedisStreamsEndpoint(c: RedisStreamsEndpointConfig, sink: boolean): void;
//# sourceMappingURL=redis.d.ts.map