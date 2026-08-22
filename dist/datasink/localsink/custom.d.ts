import { OutputDataSink, Context, type Collector, type Completion, type Consumer, type MessageContext, type RuntimeEnvironment, type Stream, type TypedSinkStream } from "../../runtime/index.js";
/** Canonical custom-sink lifecycle: getStreamId -> beginRequest -> consumeMessage -> endRequest. */
export interface EndpointHandler<HandlerState, T, R> {
    getStreamId(context: MessageContext, value: Readonly<T>): string;
    beginRequest(context: MessageContext, stream: Stream): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: Stream, handlerState: HandlerState, value: Readonly<T>, resultStream: Collector<R>): Completion;
    endRequest(context: MessageContext, stream: Stream, error: Error | undefined, handlerState: HandlerState): Completion;
}
export interface SinkCallback<T> {
    done(context: MessageContext, value: T, error: Error | undefined): Completion;
}
export declare class CustomDataSink extends OutputDataSink {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private customEndpoints;
}
export declare function makeCustomEndpointConsumer<HandlerState, T, R>(stream: TypedSinkStream<T, R>, handler: EndpointHandler<HandlerState, T, R>): Consumer<T>;
//# sourceMappingURL=custom.d.ts.map