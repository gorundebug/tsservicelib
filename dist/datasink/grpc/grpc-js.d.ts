import { type DescMethod, type DescService } from "@bufbuild/protobuf";
import { SinkStreamContext, type Completion, type Consumer, type MessageContext, type TypedSinkStreamWithResult } from "../../runtime/index.js";
export interface Sender<ReqT> {
    send(context: MessageContext, request: ReqT): Completion;
}
export interface ResultContext {
    done(): void;
}
export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
    beginRequest(context: MessageContext, stream: SinkStreamContext<T, R, E>): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: SinkStreamContext<T, R, E>, state: HandlerState, value: Readonly<T>, sender: Sender<ReqT>, result: ResultContext): Completion;
    handleResponse(context: MessageContext, stream: SinkStreamContext<T, R, E>, state: HandlerState, response: Readonly<ResR>): Completion;
    endRequest(context: MessageContext, stream: SinkStreamContext<T, R, E>, error: Error | undefined, state: HandlerState): Completion;
}
export declare function makeGrpcNoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
export declare function makeGrpcServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
export declare function makeGrpcClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
export declare function makeGrpcBidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, service: DescService, method: DescMethod, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
//# sourceMappingURL=grpc-js.d.ts.map