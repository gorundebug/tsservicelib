import { MessageContext, type Context } from "./context.js";
import type { Lifecycle } from "./lifecycle.js";
import type { StreamSerde } from "./serde/index.js";
import type { Caller, TypedStreamConsumer } from "./stream.js";
export interface DurableLinkId {
    readonly from: number;
    readonly to: number;
}
/** Portable transport envelope. Target business functions remain unchanged. */
export interface DurableEnvelope {
    readonly version: number;
    readonly from: number;
    readonly to: number;
    readonly callId: string;
    readonly streamId: string;
    readonly priority: number;
    readonly deadlineUnixMillis: number;
    readonly samplingEnabled: boolean;
    readonly payload: Uint8Array;
}
export type DurableLinkHandler = (envelope: DurableEnvelope, cancellationSignal?: AbortSignal) => Promise<void>;
/** Infrastructure transport used by DurableCall; it never replaces the target node. */
export interface DurableTransport extends Lifecycle {
    readonly id: number;
    readonly name: string;
    stopAdmission(context: Context): Promise<void>;
    registerLink(link: DurableLinkId, handler: DurableLinkHandler): void;
    submitLink(link: DurableLinkId, envelope: DurableEnvelope): Promise<void>;
}
export declare class DurableCaller<T> implements Caller<T> {
    private readonly link;
    private readonly transport;
    private readonly serde;
    constructor(link: DurableLinkId, transport: DurableTransport, serde: StreamSerde<T>);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): Promise<void>;
}
export declare function makeDurableLinkHandler<T>(consumer: TypedStreamConsumer<T>, serde: StreamSerde<T>): DurableLinkHandler;
//# sourceMappingURL=durable.d.ts.map