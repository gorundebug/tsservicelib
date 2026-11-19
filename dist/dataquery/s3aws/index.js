import { Connector } from "./connector.js";
export { Connector, makeConnector } from "./connector.js";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { S3Client, GetObjectCommand, HeadObjectCommand, ListObjectsV2Command, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { makeEndpointConsumer } from "../index.js";
/** Await native stream closure; destroying an unread stream is intentional. */
async function closeBody(body) {
    const closed = finished(body, { cleanup: true });
    body.destroy();
    try {
        await closed;
    }
    catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ERR_STREAM_PREMATURE_CLOSE"))
            throw error;
    }
}
async function withBody(body, run) {
    let failure;
    let failed = false;
    try {
        await run();
    }
    catch (error) {
        failed = true;
        failure = error;
    }
    if (body !== undefined) {
        try {
            await closeBody(body);
        }
        catch (error) {
            if (failed)
                throw new AggregateError([failure, error], "S3 operation and body cleanup failed");
            throw error;
        }
    }
    if (failed)
        throw failure;
}
async function requireSingleAttempt(client, context) {
    context.signal().throwIfAborted();
    if ((await client.config.maxAttempts()) !== 1)
        throw new Error("S3 Query requires a client configured with maxAttempts: 1");
    context.signal().throwIfAborted();
}
export function makeGetObjectEndpointConsumer(stream, source, handler) {
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
export function makeHeadObjectEndpointConsumer(stream, source, handler) {
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
export function makeListObjectsEndpointConsumer(stream, source, handler) {
    return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
        const client = source instanceof Connector ? source.client() : source;
        await requireSingleAttempt(client, context);
        const request = await handler.buildRequest(context, state, value);
        let token = request.ContinuationToken;
        do {
            context.signal().throwIfAborted();
            const page = await client.send(new ListObjectsV2Command({ ...request, ContinuationToken: token }), { abortSignal: context.signal() });
            context.signal().throwIfAborted();
            await handler.handleResult(context, state, page, out);
            if (!page.IsTruncated)
                break;
            if (!page.NextContinuationToken || page.NextContinuationToken === token)
                throw new Error("S3 listing did not advance its continuation token");
            token = page.NextContinuationToken;
        } while (true);
        context.signal().throwIfAborted();
    });
}
export function makePutObjectEndpointConsumer(stream, source, handler) {
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
export function makeDeleteObjectEndpointConsumer(stream, source, handler) {
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
//# sourceMappingURL=index.js.map