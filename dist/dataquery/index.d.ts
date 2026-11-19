import type { QueryStream } from "../operators/query.js";
import type { Collector } from "../runtime/collector.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, Consumer, Stream } from "../runtime/stream.js";
/** Per-invocation state; initial failure still reaches endRequest exactly once. */
export interface BeginRequest<HandlerState> {
    readonly context: MessageContext;
    readonly state: HandlerState;
    readonly error?: Error;
}
export interface EndpointHandler<HandlerState, R, E> {
    beginRequest(context: MessageContext, stream: Stream): BeginRequest<HandlerState> | Promise<BeginRequest<HandlerState>>;
    /** state is undefined only when beginRequest threw before returning it.
     * Emit application completion through out, and mapped failures through errors.
     * Neither collector may be retained beyond this method's completion.
     */
    endRequest(context: MessageContext, stream: Stream, state: HandlerState | undefined, error: Error | undefined, out: Collector<R>, errors: Collector<E>): Completion;
}
/** Native adapter execution. Close cursor/body before returning or throwing.
 * No replay after partial output; no detached work or private delivery queue.
 */
export type Execute<HandlerState, T, R> = (context: MessageContext, state: HandlerState, value: T, out: Collector<R>) => Completion;
/** One cooperative deadline for native execution and application mapping.
 * Zero preserves only the caller's deadline. Finalization is outside this limit.
 */
export interface QueryOptions {
    readonly operationTimeoutMs?: number;
}
export declare function makeEndpointConsumer<HandlerState, T, R, E>(stream: QueryStream<T, R, E>, handler: EndpointHandler<HandlerState, R, E>, execute: Execute<HandlerState, T, R>, options?: QueryOptions): Consumer<T>;
//# sourceMappingURL=index.d.ts.map