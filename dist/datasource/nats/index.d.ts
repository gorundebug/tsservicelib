import type { Msg } from "@nats-io/transport-node";
import type { JsMsg } from "@nats-io/jetstream";
import { InputDataSource, MessageContext, type Context, type Completion, type Consumer, type RuntimeEnvironment, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
/** Real SDK deliveries. Core has no acknowledgement operations. */
export type ConsumerMessage = {
    readonly message: Msg;
    readonly jetstream: undefined;
} | {
    readonly message: JsMsg;
    readonly jetstream: JsMsg;
};
export type ResultCallback<H, T, R, E> = (context: MessageContext, stream: StreamContext<T, R, E>, state: H, value: Readonly<R>) => boolean | Promise<boolean>;
export interface ResultContext<H, T, R, E> {
    setResultCallback(id: string, callback: ResultCallback<H, T, R, E>): void;
    /** Local lifecycle completion only, never ACK. Register callbacks before collect. */
    done(): void;
}
export interface EndpointHandler<H, T, R, E> {
    onError(context: MessageContext, stream: StreamContext<T, R, E>, error: Error): Completion;
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        context: MessageContext;
        state: H;
    } | Promise<{
        context: MessageContext;
        state: H;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, state: H, message: ConsumerMessage, result: ResultContext<H, T, R, E>): Completion;
    getMessageId(context: MessageContext, stream: StreamContext<T, R, E>, state: H, value: Readonly<R>): string;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, state: H): Completion;
}
export declare class NATSDataSource extends InputDataSource {
    constructor(id: number, environment: RuntimeEnvironment);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
}
export declare function makeNATSEndpointConsumer<H, T, R, E>(stream: TypedInputStream<T, R, E>, handler: EndpointHandler<H, T, R, E>): Consumer<T>;
//# sourceMappingURL=index.d.ts.map