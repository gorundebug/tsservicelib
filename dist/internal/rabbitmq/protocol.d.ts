import type { Publisher, Connection } from "rabbitmq-amqp-js-client";
import type { RabbitMQDataConnectorConfig, RabbitMQEndpointConfig } from "../../runtime/config/types.js";
export type NativeMessage = Parameters<Publisher["publish"]>[0];
export type NativeSettlement = Parameters<Parameters<Connection["createConsumer"]>[0]["messageHandler"]>[0];
export interface Delivery {
    readonly outcome: "accepted" | "unroutable" | "rejected" | "unknown";
    readonly nativeOutcome?: string | undefined;
    readonly remoteState?: {
        readonly condition?: string | undefined;
        readonly description?: string | undefined;
    } | undefined;
    readonly error?: Error | undefined;
}
export interface Setup {
    config: RabbitMQDataConnectorConfig;
    endpoint: RabbitMQEndpointConfig;
    source: boolean;
    server: number;
}
export type Command = {
    kind: "publish";
    id: number;
    bytes: Uint8Array;
} | {
    kind: "settle";
    id: number;
    delivery: number;
    action: "accept" | "release" | "reject";
} | {
    kind: "pause";
} | {
    kind: "close";
};
export type Event = {
    kind: "ready";
} | {
    kind: "connected";
} | {
    kind: "closed";
} | {
    kind: "error";
    error: string;
} | {
    kind: "message";
    id: number;
    bytes: Uint8Array;
} | {
    kind: "settled";
    id: number;
    error?: string;
} | {
    kind: "delivery";
    id: number;
    delivery: Delivery;
};
export declare const BYTE_LIMIT: number;
export declare const MESSAGE_LIMIT: number;
//# sourceMappingURL=protocol.d.ts.map