import { type UntypedHandleCall } from "@grpc/grpc-js";
import { type DescMethod, type DescService } from "@bufbuild/protobuf";
import { InputDataSource, Context, MessageContext, type Completion, type Consumer, type RuntimeEnvironment, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
export interface Sender<ResR> {
    send(context: MessageContext, value: ResR): Completion;
}
export type ResultCallback<HandlerState, T, ResR, R, E> = (context: MessageContext, stream: StreamContext<T, R, E>, state: HandlerState, value: Readonly<R>, sender: Sender<ResR>) => boolean | Promise<boolean>;
export interface ResultContext<HandlerState, T, ResR, R, E> {
    setResultCallback(messageId: string, callback: ResultCallback<HandlerState, T, ResR, R, E>): void;
    done(): void;
}
export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, state: HandlerState, request: Readonly<ReqT>, result: ResultContext<HandlerState, T, ResR, R, E>, sender: Sender<ResR>): Completion;
    getMessageId(context: MessageContext, stream: StreamContext<T, R, E>, state: HandlerState, value: Readonly<R>): string;
    eof(context: MessageContext, stream: StreamContext<T, R, E>, state: HandlerState): Completion;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, state: HandlerState): Completion;
}
export declare class GrpcJsDataSource extends InputDataSource {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    add(service: DescService, method: DescMethod, handler: UntypedHandleCall): void;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    stopAdmission(context: Context): Promise<void>;
}
export declare function makeGrpcNoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedInputStream<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
export declare function makeGrpcClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedInputStream<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
export declare function makeGrpcServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedInputStream<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
export declare function makeGrpcBidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedInputStream<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
//# sourceMappingURL=grpc-js.d.ts.map