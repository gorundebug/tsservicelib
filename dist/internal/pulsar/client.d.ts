import Pulsar from "pulsar-client";
import { type Context, type MessageContext, type PulsarDataConnectorConfig, type PulsarEndpointConfig } from "../../runtime/index.js";
export { cancellation, wait } from "./wait.js";
export type { ProducerMessage, Message, MessageId } from "pulsar-client";
export declare const BYTE_LIMIT: number;
export declare const MESSAGE_LIMIT: number;
export declare const PENDING_MESSAGES = 4096;
export interface Delivery {
    readonly outcome: "NotSent" | "Accepted" | "Rejected" | "Unknown";
    readonly id?: Pulsar.MessageId | undefined;
    readonly error?: Error | undefined;
}
export declare class AdmissionRejected extends Error {
    readonly outcome = "NotSent";
}
export declare function snapshot(message: Pulsar.ProducerMessage | undefined, context: MessageContext): {
    message: Pulsar.ProducerMessage;
    bytes: number;
};
export declare function messageMetadata(message: Pulsar.Message): ReadonlyMap<string, string>;
export declare function nativeFailure(reason: unknown): Delivery;
export declare class AdminError extends Error {
    readonly status: number;
    constructor(status: number);
}
export declare function ensureResources(c: PulsarDataConnectorConfig, e: PulsarEndpointConfig, source: boolean, context: Context): Promise<void>;
export declare class Session {
    #private;
    readonly config: PulsarDataConnectorConfig;
    readonly ep: PulsarEndpointConfig;
    readonly source: boolean;
    private readonly report;
    ready: boolean;
    stopped: boolean;
    epoch: number;
    readonly limit: number;
    constructor(config: PulsarDataConnectorConfig, ep: PulsarEndpointConfig, source: boolean, report: (error: Error) => void);
    isReady(): boolean;
    private isStopped;
    start(context: Context): Promise<void>;
    receive(context: Context): Promise<Pulsar.Message>;
    settle(message: Pulsar.Message, epoch: number, context: Context, nack?: boolean): Promise<void>;
    publish(message: Pulsar.ProducerMessage, context: MessageContext): Promise<Delivery>;
    close(context: Context): Promise<void>;
}
//# sourceMappingURL=client.d.ts.map