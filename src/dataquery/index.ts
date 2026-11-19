import { endpointMetrics } from "./metrics.js";
import type { QueryStream } from "../operators/query.js";
import { FunctionCollector } from "../runtime/collector.js";
import type { Collector } from "../runtime/collector.js";
import type { MessageContext } from "../runtime/context.js";
import { errorFromUnknown } from "../runtime/errors.js";
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
  endRequest(context: MessageContext, stream: Stream, state: HandlerState | undefined, error: Error | undefined,
    out: Collector<R>, errors: Collector<E>): Completion;
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

export function makeEndpointConsumer<HandlerState, T, R, E>(
  stream: QueryStream<T, R, E>, handler: EndpointHandler<HandlerState, R, E>, execute: Execute<HandlerState, T, R>, options: QueryOptions = {}
): Consumer<T> {
  const timeout = options.operationTimeoutMs ?? 0;
  if (!Number.isSafeInteger(timeout) || timeout < 0 || timeout > 2147483647) {
    throw new Error("Query operation timeout must be an integer in 0..2147483647");
  }
  const telemetry = endpointMetrics(stream);
  const consumer: Consumer<T> = {
    async consume(context, value): Promise<void> {
      const started = telemetry === undefined ? undefined : performance.now();
      telemetry?.requests.inc(context);
      telemetry?.active.inc();
      const out = new FunctionCollector<R>(telemetry === undefined
        ? (ctx, result) => stream.consumeResult(ctx, result)
        : (ctx, result) => { telemetry.results.inc(ctx); return stream.consumeResult(ctx, result); });
      const errors = new FunctionCollector<E>((ctx, error) => stream.errorStream().emit(ctx, error));
      let handlerContext = context;
      let state: HandlerState | undefined;
      let error: Error | undefined;
      let cancelled = false;
      try {
        try {
          const begun = await handler.beginRequest(context, stream);
          state = begun.state;
          if (begun.context === undefined || begun.context === null) {
            error = begun.error ?? new Error("Query beginRequest returned no context");
          } else {
            handlerContext = begun.context;
            error = begun.error;
          }
          if (error === undefined) {
            handlerContext.signal().throwIfAborted();
            const scope = timeout > 0 ? new AbortController() : undefined;
            const operationContext = scope === undefined ? handlerContext
              : handlerContext.withExternalCancellation(scope.signal).bounded(timeout);
            try {
              operationContext.signal().throwIfAborted();
              await execute(operationContext, begun.state, value, out);
              operationContext.signal().throwIfAborted();
            } catch (cause) {
              cancelled = operationContext.signal().aborted;
              throw cause;
            } finally {
              // Uses the existing context timer/listener cleanup. Never race the
              // operation promise: its native cleanup completes before endRequest.
              scope?.abort();
            }
          }
        } catch (cause) {
          error = errorFromUnknown(cause);
        }
        await handler.endRequest(handlerContext, stream, state, error, out, errors);
      } finally {
        if (telemetry !== undefined && started !== undefined) {
          telemetry.active.dec();
          telemetry.duration.observe(context, (performance.now() - started) / 1000);
          if (error !== undefined) {
            const counter = cancelled || handlerContext.cancelled() ? telemetry.cancellations : telemetry.errors;
            counter.inc(context);
          }
        }
      }
    }
  };
  stream.setQueryConsumer(consumer);
  return consumer;
}
