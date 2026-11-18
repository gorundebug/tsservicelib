import type { DataConnectorConfig, EndpointConfig, RabbitMQDataConnectorConfig, RabbitMQEndpointConfig } from "./types.js";
export declare function requireRabbitMQDataConnectorConfig(value: DataConnectorConfig | RabbitMQDataConnectorConfig | undefined): RabbitMQDataConnectorConfig;
export declare function requireRabbitMQEndpointConfig(value: EndpointConfig | undefined): RabbitMQEndpointConfig;
export declare function validateRabbitMQEndpoint(c: RabbitMQEndpointConfig, sink: boolean): void;
//# sourceMappingURL=rabbitmq.d.ts.map