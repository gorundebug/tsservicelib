import { makeEndpointConsumer } from "../index.js";
/** Keep native work and its cancellation inside the request lifetime.
 * Kysely's high-level signal option may release in the background after abort.
 * Here provideConnection owns release and we await both execution and cancellation
 * before returning, so EndRequest cannot race a still-running cursor.
 */
async function withConnection(context, db, execute) {
    const signal = context.signal();
    signal.throwIfAborted();
    const executor = db.getExecutor();
    // Deliberately do not pass AbortableOperationOptions here: Kysely 0.29.9
    // returns on abort and releases a late-acquired connection in a background
    // continuation (RuntimeDriver.acquireConnection). Query must finish cleanup
    // before endRequest. Pool acquisition can therefore finish after the Query
    // deadline; connectTimeoutMs independently bounds native pg pool waiting.
    return executor.provideConnection(async (connection) => {
        signal.throwIfAborted();
        await connection.collectSessionInfo?.();
        signal.throwIfAborted();
        let cancellation;
        const abort = () => {
            // PostgresDialect uses its official controlClient, outside the busy pool.
            // The fallback is part of the public Kysely connection contract.
            cancellation ??= connection.cancelQuery?.((run) => executor.provideConnection(run)) ?? Promise.resolve();
            // Observe immediately; the original promise is still awaited in finally.
            void cancellation.catch(() => undefined);
        };
        signal.addEventListener("abort", abort, { once: true });
        try {
            signal.throwIfAborted();
            const result = await execute(connection);
            signal.throwIfAborted();
            return result;
        }
        catch (error) {
            signal.throwIfAborted();
            throw error;
        }
        finally {
            signal.removeEventListener("abort", abort);
            if (cancellation !== undefined)
                await cancellation;
        }
    });
}
export function makeRowsEndpointConsumer(stream, db, handler, fetchSize = 256, options = {}) {
    if (!Number.isSafeInteger(fetchSize) || fetchSize <= 0)
        throw new Error("Query fetchSize must be a positive integer");
    return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
        const query = await handler.buildQuery(context, state, value, db);
        await withConnection(context, db, async (connection) => {
            for await (const result of connection.streamQuery(query, fetchSize)) {
                for (const row of result.rows) {
                    context.signal().throwIfAborted();
                    await handler.handleRow(context, state, row, out);
                }
            }
        });
    }, options);
}
export function makeExecuteEndpointConsumer(stream, db, handler, options = {}) {
    return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
        const query = await handler.buildQuery(context, state, value, db);
        // Native autocommit finishes before application mapping. No write replay.
        const result = await withConnection(context, db, (connection) => connection.executeQuery(query));
        await handler.handleResult(context, state, result, out);
    }, options);
}
export { Connector, makeConnector } from "./connector.js";
//# sourceMappingURL=index.js.map