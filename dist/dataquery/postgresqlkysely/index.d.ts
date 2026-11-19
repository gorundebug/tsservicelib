import type { CompiledQuery, Kysely, QueryResult } from "kysely";
import type { EndpointHandler, QueryOptions } from "../index.js";
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
export declare function makeRowsEndpointConsumer<DB, HandlerState, T, Row, R, E>(stream: QueryStream<T, R, E>, db: Kysely<DB>, handler: RowsHandler<DB, HandlerState, T, Row, R, E>, fetchSize?: number, options?: QueryOptions): Consumer<T>;
export declare function makeExecuteEndpointConsumer<DB, HandlerState, T, Row, R, E>(stream: QueryStream<T, R, E>, db: Kysely<DB>, handler: ExecuteHandler<DB, HandlerState, T, Row, R, E>, options?: QueryOptions): Consumer<T>;
export { Connector, makeConnector } from "./connector.js";
export type { PostgreSQLKyselyConfig } from "./connector.js";
//# sourceMappingURL=index.d.ts.map