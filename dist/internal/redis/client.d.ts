import { Context, type MessageContext, type RedisStreamsDataConnectorConfig, type RedisStreamsEndpointConfig } from "../../runtime/index.js";
export { wait, cancellation } from "./wait.js";
export declare const PENDING_MESSAGES = 256;
export declare const BYTE_LIMIT: number;
export declare const MESSAGE_LIMIT: number;
export type FieldValue = readonly [string | Uint8Array, string | Uint8Array];
export type Fields = readonly FieldValue[];
export interface Entry {
    readonly id: string;
    readonly fields: readonly (readonly [Buffer, Buffer])[];
    readonly reclaimed: boolean;
    readonly bytes: number;
}
export interface Delivery {
    readonly outcome: "published" | "rejected" | "unknown" | "not-sent";
    readonly id?: string | undefined;
    readonly error?: Error | undefined;
}
export declare class DeletedPendingEntriesError extends Error {
    readonly ids: readonly string[];
    constructor(ids: readonly string[]);
}
export declare function entryBytes(fields: Fields): number;
export declare function snapshot(fields: Fields, context: MessageContext): {
    fields: Fields;
    bytes: number;
};
export declare function entryMetadata(entry: Entry): ReadonlyMap<string, string>;
export declare class Session {
    #private;
    readonly config: RedisStreamsDataConnectorConfig;
    readonly ep: RedisStreamsEndpointConfig;
    readonly source: boolean;
    ready: boolean;
    epoch: number;
    stopped: boolean;
    paused: boolean;
    readonly limit: number;
    readonly consumerName: string;
    constructor(config: RedisStreamsDataConnectorConfig, ep: RedisStreamsEndpointConfig, source: boolean, report: (error: Error) => void);
    isReady(): boolean;
    private isStopped;
    start(context: Context): Promise<void>;
    private open;
    private direct;
    invalidate(error: Error): void;
    private command;
    publish(fields: Fields, context: Context): Promise<Delivery>;
    ack(id: string, epoch: number, context: Context): Promise<number>;
    read(count: number, context: Context, block?: number): Promise<Entry[]>;
    claim(cursor: string, count: number, context: Context): Promise<{
        cursor: string;
        entries: Entry[];
        deleted: readonly string[];
    }>;
    pause(): void;
    close(): void;
}
//# sourceMappingURL=client.d.ts.map