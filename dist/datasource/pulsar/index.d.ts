import { InputDataSource, MessageContext, type Context, type Completion, type Consumer, type RuntimeEnvironment, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
import { Session, type Message } from "../../internal/pulsar/client.js";
/** Individual native settlement; Done never calls these methods. */
export declare class ConsumerMessage {
    #private;
    readonly message: Message;
    private readonly session;
    private readonly epoch;
    private readonly context;
    constructor(message: Message, session: Session, epoch: number, context: Context);
    retire(): void;
    private settle;
    ack(): Promise<void>;
    nack(): Promise<void>;
}
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
export declare class PulsarDataSource extends InputDataSource {
    constructor(id: number, environment: RuntimeEnvironment);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
}
export declare function makePulsarEndpointConsumer<H, T, R, E>(stream: TypedInputStream<T, R, E>, handler: EndpointHandler<H, T, R, E>): Consumer<T>;
export type { Message } from "../../internal/pulsar/client.js";
//# sourceMappingURL=index.d.ts.map