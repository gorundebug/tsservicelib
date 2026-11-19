import type { CompiledQuery, DatabaseConnection, Kysely, QueryResult } from "kysely";
import type { EndpointHandler, QueryOptions } from "../index.js";
import { makeEndpointConsumer } from "../index.js";
import type { QueryStream } from "../../operators/query.js";
import type { Collector } from "../../runtime/collector.js";
import type { MessageContext } from "../../runtime/context.js";
import type { Completion, Consumer } from "../../runtime/stream.js";

export interface RowsHandler<DB, HandlerState, T, Row, R, E> extends EndpointHandler<HandlerState, R, E> {
  /** Compile a Kysely expression (or sql template) with separate parameters. */
  buildQuery(context: MessageContext, state: HandlerState, value: T, db: Kysely<DB>): CompiledQuery<Row> | Promise<CompiledQuery<Row>>;
  handleRow(context: MessageContext, state: HandlerState, row: Row, out: Collector<R>): Completion;
}
export interface ExecuteHandler<DB, HandlerState, T, Row, R, E> extends EndpointHandler<HandlerState, R, E> {
  buildQuery(context: MessageContext, state: HandlerState, value: T, db: Kysely<DB>): CompiledQuery<Row> | Promise<CompiledQuery<Row>>;
  handleResult(context: MessageContext, state: HandlerState, result: QueryResult<Row>, out: Collector<R>): Completion;
}

/** Keep native work and its cancellation inside the request lifetime.
 * Kysely's high-level signal option may release in the background after abort.
 * Here provideConnection owns release and we await both execution and cancellation
 * before returning, so EndRequest cannot race a still-running cursor.
 */
async function withConnection<DB, Result>(
  context: MessageContext, db: Kysely<DB>, execute: (connection: DatabaseConnection) => Promise<Result>
): Promise<Result> {
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
    let cancellation: Promise<void> | undefined;
    const abort = (): void => {
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
    } catch (error) {
      signal.throwIfAborted();
      throw error;
    } finally {
      signal.removeEventListener("abort", abort);
      if (cancellation !== undefined) await cancellation;
    }
  });
}

export function makeRowsEndpointConsumer<DB, HandlerState, T, Row, R, E>(
  stream: QueryStream<T, R, E>, db: Kysely<DB>, handler: RowsHandler<DB, HandlerState, T, Row, R, E>, fetchSize = 256, options: QueryOptions = {}
): Consumer<T> {
  if (!Number.isSafeInteger(fetchSize) || fetchSize <= 0) throw new Error("Query fetchSize must be a positive integer");
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const query = await handler.buildQuery(context, state, value, db);
    await withConnection(context, db, async (connection) => {
      for await (const result of connection.streamQuery<Row>(query, fetchSize)) {
        for (const row of result.rows) {
          context.signal().throwIfAborted();
          await handler.handleRow(context, state, row, out);
        }
      }
    });
  }, options);
}

export function makeExecuteEndpointConsumer<DB, HandlerState, T, Row, R, E>(
  stream: QueryStream<T, R, E>, db: Kysely<DB>, handler: ExecuteHandler<DB, HandlerState, T, Row, R, E>, options: QueryOptions = {}
): Consumer<T> {
  return makeEndpointConsumer(stream, handler, async (context, state, value, out) => {
    const query = await handler.buildQuery(context, state, value, db);
    // Native autocommit finishes before application mapping. No write replay.
    const result = await withConnection(context, db, (connection) => connection.executeQuery<Row>(query));
    await handler.handleResult(context, state, result, out);
  }, options);
}

export { Connector, makeConnector } from "./connector.js";
export type { PostgreSQLKyselyConfig } from "./connector.js";
