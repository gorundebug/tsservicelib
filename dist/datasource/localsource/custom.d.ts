import { InputDataSource, Context, type Completion, type Consumer, type MessageContext, type RuntimeEnvironment, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
export interface DataProducer<T> {
    start(context: Context, consumer: Consumer<T>): Completion;
    stop(context: Context): Completion;
}
export type ResultCallback<HandlerState, T, R, E> = (context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<R>) => boolean | Promise<boolean>;
export interface ResultContext<HandlerState, T, R, E> {
    setResultCallback(messageId: string, callback: ResultCallback<HandlerState, T, R, E>): void;
    done(): void;
}
export interface EndpointHandler<HandlerState, T, R, E> {
    concurrency(stream: StreamContext<T, R, E>): number;
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<T>, result: ResultContext<HandlerState, T, R, E>): Completion;
    getMessageId(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<R>): string;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, handlerState: HandlerState): Completion;
}
export declare class CustomDataSource extends InputDataSource {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private customEndpoints;
}
export declare function makeCustomEndpointConsumer<HandlerState, T, R, E>(stream: TypedInputStream<T, R, E>, producer: DataProducer<T>, handler: EndpointHandler<HandlerState, T, R, E>): Consumer<T>;
//# sourceMappingURL=custom.d.ts.map