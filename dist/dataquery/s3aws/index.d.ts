import { Connector } from "./connector.js";
export { Connector, makeConnector } from "./connector.js";
import { S3Client, type GetObjectCommandInput, type GetObjectCommandOutput, type HeadObjectCommandInput, type HeadObjectCommandOutput, type ListObjectsV2CommandInput, type ListObjectsV2CommandOutput, type PutObjectCommandInput, type PutObjectCommandOutput, type DeleteObjectCommandInput, type DeleteObjectCommandOutput } from "@aws-sdk/client-s3";
import { type EndpointHandler } from "../index.js";
import type { QueryStream } from "../../operators/query.js";
import type { Collector } from "../../runtime/collector.js";
import type { MessageContext } from "../../runtime/context.js";
import type { Completion, Consumer } from "../../runtime/stream.js";
export interface GetObjectHandler<HandlerState, T, R, E> extends EndpointHandler<HandlerState, R, E> {
    buildRequest(context: MessageContext, state: HandlerState, value: T): GetObjectCommandInput | Promise<GetObjectCommandInput>;
    /** Map synchronously within the invocation; do not retain the response/body. */
    handleResult(context: MessageContext, state: HandlerState, result: GetObjectCommandOutput, out: Collector<R>): Completion;
}
export declare function makeGetObjectEndpointConsumer<HandlerState, T, R, E>(stream: QueryStream<T, R, E>, source: S3Client | Connector, handler: GetObjectHandler<HandlerState, T, R, E>): Consumer<T>;
export interface HeadObjectHandler<HandlerState, T, R, E> extends EndpointHandler<HandlerState, R, E> {
    buildRequest(context: MessageContext, state: HandlerState, value: T): HeadObjectCommandInput | Promise<HeadObjectCommandInput>;
    /** Map synchronously within the invocation; do not retain the response/body. */
    handleResult(context: MessageContext, state: HandlerState, result: HeadObjectCommandOutput, out: Collector<R>): Completion;
}
export declare function makeHeadObjectEndpointConsumer<HandlerState, T, R, E>(stream: QueryStream<T, R, E>, source: S3Client | Connector, handler: HeadObjectHandler<HandlerState, T, R, E>): Consumer<T>;
export interface ListObjectsHandler<HandlerState, T, R, E> extends EndpointHandler<HandlerState, R, E> {
    buildRequest(context: MessageContext, state: HandlerState, value: T): ListObjectsV2CommandInput | Promise<ListObjectsV2CommandInput>;
    /** Map synchronously within the invocation; do not retain the response/body. */
    handleResult(context: MessageContext, state: HandlerState, result: ListObjectsV2CommandOutput, out: Collector<R>): Completion;
}
export declare function makeListObjectsEndpointConsumer<HandlerState, T, R, E>(stream: QueryStream<T, R, E>, source: S3Client | Connector, handler: ListObjectsHandler<HandlerState, T, R, E>): Consumer<T>;
export interface PutObjectHandler<HandlerState, T, R, E> extends EndpointHandler<HandlerState, R, E> {
    buildRequest(context: MessageContext, state: HandlerState, value: T): PutObjectCommandInput | Promise<PutObjectCommandInput>;
    /** Map synchronously within the invocation; do not retain the response/body. */
    handleResult(context: MessageContext, state: HandlerState, result: PutObjectCommandOutput, out: Collector<R>): Completion;
}
export declare function makePutObjectEndpointConsumer<HandlerState, T, R, E>(stream: QueryStream<T, R, E>, source: S3Client | Connector, handler: PutObjectHandler<HandlerState, T, R, E>): Consumer<T>;
export interface DeleteObjectHandler<HandlerState, T, R, E> extends EndpointHandler<HandlerState, R, E> {
    buildRequest(context: MessageContext, state: HandlerState, value: T): DeleteObjectCommandInput | Promise<DeleteObjectCommandInput>;
    /** Map synchronously within the invocation; do not retain the response/body. */
    handleResult(context: MessageContext, state: HandlerState, result: DeleteObjectCommandOutput, out: Collector<R>): Completion;
}
export declare function makeDeleteObjectEndpointConsumer<HandlerState, T, R, E>(stream: QueryStream<T, R, E>, source: S3Client | Connector, handler: DeleteObjectHandler<HandlerState, T, R, E>): Consumer<T>;
//# sourceMappingURL=index.d.ts.map