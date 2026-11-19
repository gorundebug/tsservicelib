import { Connector } from "./connector.js";
export { Connector, makeConnector } from "./connector.js";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  DeleteObjectCommand,
  type GetObjectCommandInput,
  type GetObjectCommandOutput,
  type HeadObjectCommandInput,
  type HeadObjectCommandOutput,
  type ListObjectsV2CommandInput,
  type ListObjectsV2CommandOutput,
  type PutObjectCommandInput,
  type PutObjectCommandOutput,
  type DeleteObjectCommandInput,
  type DeleteObjectCommandOutput
} from "@aws-sdk/client-s3";
import { makeEndpointConsumer, type EndpointHandler } from "../index.js";
import type { QueryStream } from "../../operators/query.js";
import type { Collector } from "../../runtime/collector.js";
import type { MessageContext } from "../../runtime/context.js";
import type { Completion, Consumer } from "../../runtime/stream.js";

/** Await native stream closure; destroying an unread stream is intentional. */
async function closeBody(body: Readable): Promise<void> {
  const closed = finished(body, { cleanup: true });
  body.destroy();
  try {
    await closed;
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ERR_STREAM_PREMATURE_CLOSE"))
      throw error;
  }
}
async function withBody(body: Readable | undefined, run: () => Promise<void>): Promise<void> {
  let failure: unknown;
  let failed = false;
  try {
    await run();
  } catch (error) {
    failed = true;
    failure = error;
  }
  if (body !== undefined) {
    try {
      await closeBody(body);
    } catch (error) {
      if (failed)
        throw new AggregateError([failure, error], "S3 operation and body cleanup failed");
      throw error;
    }
  }
  if (failed) throw failure;
}
async function requireSingleAttempt(client: S3Client, context: MessageContext): Promise<void> {
  context.signal().throwIfAborted();
  if ((await client.config.maxAttempts()) !== 1)
    throw new Error("S3 Query requires a client configured with maxAttempts: 1");
  context.signal().throwIfAborted();
}

export interface GetObjectHandler<HandlerState, T, R, E> extends EndpointHandler<
  HandlerState,
  R,
  E
> {
  buildRequest(
    context: MessageContext,
    state: HandlerState,
    value: T
  ): GetObjectCommandInput | Promise<GetObjectCommandInput>;
  /** Map synchronously within the invocation; do not retain the response/body. */
  handleResult(
    context: MessageContext,
    state: HandlerState,
    result: GetObjectCommandOutput,
    out: Collector<R>
  ): Completion;
}

export function makeGetObjectEndpointConsumer<HandlerState, T, R, E>(
  stream: QueryStream<T, R, E>,
  source: S3Client | Connector,
  handler: GetObjectHandler<HandlerState, T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const client = source instanceof Connector ? source.client() : source;
    await requireSingleAttempt(client, context);
    const request = await handler.buildRequest(context, state, value);
    context.signal().throwIfAborted();
    const result = await client.send(new GetObjectCommand(request), {
      abortSignal: context.signal()
    });
    if (!(result.Body instanceof Readable))
      throw new Error("S3 Node.js GetObject requires a native readable body");
    await withBody(result.Body, async () => {
      context.signal().throwIfAborted();
      await handler.handleResult(context, state, result, out);
      context.signal().throwIfAborted();
    });
  });
}

export interface HeadObjectHandler<HandlerState, T, R, E> extends EndpointHandler<
  HandlerState,
  R,
  E
> {
  buildRequest(
    context: MessageContext,
    state: HandlerState,
    value: T
  ): HeadObjectCommandInput | Promise<HeadObjectCommandInput>;
  /** Map synchronously within the invocation; do not retain the response/body. */
  handleResult(
    context: MessageContext,
    state: HandlerState,
    result: HeadObjectCommandOutput,
    out: Collector<R>
  ): Completion;
}

export function makeHeadObjectEndpointConsumer<HandlerState, T, R, E>(
  stream: QueryStream<T, R, E>,
  source: S3Client | Connector,
  handler: HeadObjectHandler<HandlerState, T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const client = source instanceof Connector ? source.client() : source;
    await requireSingleAttempt(client, context);
    const request = await handler.buildRequest(context, state, value);
    context.signal().throwIfAborted();
    const result = await client.send(new HeadObjectCommand(request), {
      abortSignal: context.signal()
    });
    context.signal().throwIfAborted();
    await handler.handleResult(context, state, result, out);
    context.signal().throwIfAborted();
  });
}

export interface ListObjectsHandler<HandlerState, T, R, E> extends EndpointHandler<
  HandlerState,
  R,
  E
> {
  buildRequest(
    context: MessageContext,
    state: HandlerState,
    value: T
  ): ListObjectsV2CommandInput | Promise<ListObjectsV2CommandInput>;
  /** Map synchronously within the invocation; do not retain the response/body. */
  handleResult(
    context: MessageContext,
    state: HandlerState,
    result: ListObjectsV2CommandOutput,
    out: Collector<R>
  ): Completion;
}

export function makeListObjectsEndpointConsumer<HandlerState, T, R, E>(
  stream: QueryStream<T, R, E>,
  source: S3Client | Connector,
  handler: ListObjectsHandler<HandlerState, T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const client = source instanceof Connector ? source.client() : source;
    await requireSingleAttempt(client, context);
    const request = await handler.buildRequest(context, state, value);
    let token = request.ContinuationToken;
    do {
      context.signal().throwIfAborted();
      const page = await client.send(
        new ListObjectsV2Command({ ...request, ContinuationToken: token }),
        { abortSignal: context.signal() }
      );
      context.signal().throwIfAborted();
      await handler.handleResult(context, state, page, out);
      if (!page.IsTruncated) break;
      if (!page.NextContinuationToken || page.NextContinuationToken === token)
        throw new Error("S3 listing did not advance its continuation token");
      token = page.NextContinuationToken;
    } while (true);
    context.signal().throwIfAborted();
  });
}

export interface PutObjectHandler<HandlerState, T, R, E> extends EndpointHandler<
  HandlerState,
  R,
  E
> {
  buildRequest(
    context: MessageContext,
    state: HandlerState,
    value: T
  ): PutObjectCommandInput | Promise<PutObjectCommandInput>;
  /** Map synchronously within the invocation; do not retain the response/body. */
  handleResult(
    context: MessageContext,
    state: HandlerState,
    result: PutObjectCommandOutput,
    out: Collector<R>
  ): Completion;
}

export function makePutObjectEndpointConsumer<HandlerState, T, R, E>(
  stream: QueryStream<T, R, E>,
  source: S3Client | Connector,
  handler: PutObjectHandler<HandlerState, T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const client = source instanceof Connector ? source.client() : source;
    await requireSingleAttempt(client, context);
    const request = await handler.buildRequest(context, state, value);
    // The successful builder transfers ownership of a Node upload stream.
    await withBody(request.Body instanceof Readable ? request.Body : undefined, async () => {
      context.signal().throwIfAborted();
      const result = await client.send(new PutObjectCommand(request), {
        abortSignal: context.signal()
      });
      context.signal().throwIfAborted();
      await handler.handleResult(context, state, result, out);
      context.signal().throwIfAborted();
    });
  });
}

export interface DeleteObjectHandler<HandlerState, T, R, E> extends EndpointHandler<
  HandlerState,
  R,
  E
> {
  buildRequest(
    context: MessageContext,
    state: HandlerState,
    value: T
  ): DeleteObjectCommandInput | Promise<DeleteObjectCommandInput>;
  /** Map synchronously within the invocation; do not retain the response/body. */
  handleResult(
    context: MessageContext,
    state: HandlerState,
    result: DeleteObjectCommandOutput,
    out: Collector<R>
  ): Completion;
}

export function makeDeleteObjectEndpointConsumer<HandlerState, T, R, E>(
  stream: QueryStream<T, R, E>,
  source: S3Client | Connector,
  handler: DeleteObjectHandler<HandlerState, T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const client = source instanceof Connector ? source.client() : source;
    await requireSingleAttempt(client, context);
    const request = await handler.buildRequest(context, state, value);
    context.signal().throwIfAborted();
    const result = await client.send(new DeleteObjectCommand(request), {
      abortSignal: context.signal()
    });
    context.signal().throwIfAborted();
    await handler.handleResult(context, state, result, out);
    context.signal().throwIfAborted();
  });
}
