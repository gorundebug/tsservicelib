import { type IncomingHttpHeaders, type IncomingMessage } from "node:http";
import { Readable } from "node:stream";
import { type MessageContext, OutputDataSink, SinkStreamContext, type Completion, type Consumer, type Context, type HttpDataConnectorConfig, type HttpEndpointConfig, type RuntimeEnvironment, type TypedSinkStreamWithResult } from "../../runtime/index.js";
export type RequestBody = string | Uint8Array | Readable | undefined;
export declare class Request {
    readonly context: MessageContext;
    readonly method: string;
    readonly url: URL;
    readonly headers: Headers;
    readonly body: RequestBody;
    constructor(context: MessageContext, method: string, url: string | URL, body?: RequestBody);
}
export declare class Requester {
    #private;
    newRequest(context: MessageContext, method: string, url: string | URL, body?: RequestBody): Request;
    request(): Request | undefined;
}
export declare class Response {
    readonly statusCode: number;
    readonly status: string;
    readonly headers: IncomingHttpHeaders;
    readonly body: IncomingMessage;
    constructor(body: IncomingMessage);
    read(maxBytes?: number): Promise<Uint8Array>;
    text(maxBytes?: number): Promise<string>;
    close(): Promise<void>;
}
export declare class ResponseBodyTooLargeError extends Error {
    readonly limit: number;
    constructor(limit: number);
}
export interface Client {
    do(request: Request): Promise<Response>;
    close(context: Context): Promise<void>;
}
export interface NodeHttpClientOptions {
    readonly maxSockets?: number;
    readonly maxFreeSockets?: number;
}
export declare class NodeHttpClient implements Client {
    #private;
    constructor(options?: NodeHttpClientOptions);
    do(request: Request): Promise<Response>;
    close(context: Context): Promise<void>;
}
export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
    beginRequest(context: MessageContext, stream: StreamContext<T, R, E>): {
        readonly context: MessageContext;
        readonly state: HandlerState;
    } | Promise<{
        readonly context: MessageContext;
        readonly state: HandlerState;
    }>;
    consumeMessage(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, value: Readonly<T>, requester: Requester): Completion;
    handleResponse(context: MessageContext, stream: StreamContext<T, R, E>, handlerState: HandlerState, response: Readonly<Response>): Completion;
    endRequest(context: MessageContext, stream: StreamContext<T, R, E>, error: Error | undefined, handlerState: HandlerState): Completion;
}
export declare class StreamContext<T, R, E> extends SinkStreamContext<T, R, E> {
    #private;
    constructor(stream: TypedSinkStreamWithResult<T, R, E>);
    get endpointConfig(): HttpEndpointConfig;
    get dataConnectorConfig(): HttpDataConnectorConfig;
}
export declare class NodeHttpDataSink extends OutputDataSink {
    #private;
    constructor(connectorId: number, environment: RuntimeEnvironment, client: Client);
    client(): Client;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private httpEndpoints;
    private stopEndpoints;
}
export declare function makeNodeHttpEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(stream: TypedSinkStreamWithResult<T, R, E>, client: Client, handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>): Consumer<T>;
//# sourceMappingURL=node-http.d.ts.map