import { type MsgHdrs, type NatsConnection } from "@nats-io/transport-node";
import { type Consumer, type JetStreamClient } from "@nats-io/jetstream";
import { type Context, type NATSDataConnectorConfig, type NATSEndpointConfig } from "../../runtime/index.js";
export declare const PENDING_MESSAGES = 256;
export declare const PENDING_BYTES: number;
export declare function cancellation(context: Context): Error;
/** Observes late rejection, removes listeners, and never interprets cancellation as withdrawal. */
export declare function wait<T>(operation: Promise<T>, context: Context, timeoutMs?: number): Promise<T>;
export declare function openConnection(config: NATSDataConnectorConfig, owner: Context): Promise<NatsConnection>;
/** Checks containment of the whole endpoint subject language, not just one concrete sample. */
export declare function subjectCovered(filter: string, subject: string): boolean;
export declare function resources(nc: NatsConnection, config: NATSEndpointConfig, timeout: number, source: boolean): Promise<{
    js: JetStreamClient;
    consumer?: Consumer;
}>;
export declare function messageBytes(data: Uint8Array, subject: string, headers?: MsgHdrs): number;
//# sourceMappingURL=client.d.ts.map