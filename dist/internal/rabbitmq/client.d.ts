import { Worker } from "node:worker_threads";
import { Context, type MessageContext, type RabbitMQDataConnectorConfig, type RabbitMQEndpointConfig } from "../../runtime/index.js";
import { type Delivery, type NativeMessage } from "./protocol.js";
export { wait, cancellation } from "./wait.js";
export { BYTE_LIMIT, MESSAGE_LIMIT, type Delivery, type NativeMessage } from "./protocol.js";
export declare const PENDING_MESSAGES = 256;
export declare class Session {
    #private;
    readonly config: RabbitMQDataConnectorConfig;
    readonly ep: RabbitMQEndpointConfig;
    readonly source: boolean;
    private readonly report;
    private readonly receive;
    worker: Worker | undefined;
    ready: boolean;
    epoch: number;
    stopped: boolean;
    paused: boolean;
    readonly limit: number;
    constructor(config: RabbitMQDataConnectorConfig, ep: RabbitMQEndpointConfig, source: boolean, report: (error: Error) => void, receive?: (message: NativeMessage, id: number, epoch: number) => void);
    start(context: Context): Promise<void>;
    private open;
    invalidate(error: Error): void;
    private command;
    publish(encoded: Uint8Array, context: Context): Promise<Delivery>;
    settle(id: number, epoch: number, action: "accept" | "release" | "reject", context: Context): Promise<void>;
    pause(): void;
    close(context: Context): Promise<void>;
    private finishClose;
}
export declare function snapshot(message: NativeMessage, context: MessageContext): Uint8Array;
//# sourceMappingURL=client.d.ts.map