import { type IncomingMessage, type ServerResponse } from "node:http";
import { type Context, DataSourceEndpoint, InputDataSource, MessageContext, type Completion, type Consumer, type HttpEndpointConfig, type HTTPHandler, type InputEndpointConsumer, type RuntimeEnvironment, type StreamContext, type TypedInputStream } from "../../runtime/index.js";
export type { HTTPHandler } from "../../runtime/index.js";
export interface HandlerData {
    readonly request: IncomingMessage;
    readonly response: ServerResponse;
}
export type ResultCallback<HandlerState, ReqT, ResR, T, R, E> = (context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<R>, data: HandlerData) => boolean | Promise<boolean>;
export interface ResultContext<HandlerState, ReqT, ResR, T, R, E> {
    setResultCallback(messageId: string, callback: ResultCallback<HandlerState, ReqT, ResR, T, R, E>): void;
    done(): void;
}
export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>, data: HandlerData): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, data: HandlerData, resultContext: ResultContext<HandlerState, ReqT, ResR, T, R, E>): Completion;
    getMessageId(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<R>): string;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, handlerState: HandlerState, data: HandlerData): Completion;
}
declare class NodeHttpInputEndpoint extends DataSourceEndpoint {
    #private;
    readonly method: "GET" | "POST";
    readonly path: string;
    constructor(dataSource: NodeHttpDataSource, config: HttpEndpointConfig);
    bindConsumer(consumer: NodeHttpEndpointConsumerContract): void;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    stopAdmission(): void;
    handler(): HTTPHandler;
    private serve;
}
interface NodeHttpEndpointConsumerContract extends InputEndpointConsumer {
    start(context: Context): Promise<void>;
    stopAdmission(): void;
    stop(context: Context): Promise<void>;
    serveHttp(request: IncomingMessage, response: ServerResponse): Promise<void>;
}
export declare class NodeHttpDataSource extends InputDataSource {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment);
    addHttpEndpoint(endpoint: NodeHttpInputEndpoint): void;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    stopAdmission(context: Context): Promise<void>;
    private httpEndpoints;
    private stopEndpoints;
    private route;
}
export declare function makeNodeHttpEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedInputStream<T, R, E>, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): readonly [consumer: Consumer<T>, handler: HTTPHandler];
//# sourceMappingURL=node-http.d.ts.map