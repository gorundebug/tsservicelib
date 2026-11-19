import { endpointMetrics } from "./metrics.js";
import { FunctionCollector } from "../runtime/collector.js";
import { errorFromUnknown } from "../runtime/errors.js";
export function makeEndpointConsumer(stream, handler, execute, options = {}) {
    const timeout = options.operationTimeoutMs ?? 0;
    if (!Number.isSafeInteger(timeout) || timeout < 0 || timeout > 2147483647) {
        throw new Error("Query operation timeout must be an integer in 0..2147483647");
    }
    const telemetry = endpointMetrics(stream);
    const consumer = {
        async consume(context, value) {
            const started = telemetry === undefined ? undefined : performance.now();
            telemetry?.requests.inc(context);
            telemetry?.active.inc();
            const out = new FunctionCollector(telemetry === undefined
                ? (ctx, result) => stream.consumeResult(ctx, result)
                : (ctx, result) => { telemetry.results.inc(ctx); return stream.consumeResult(ctx, result); });
            const errors = new FunctionCollector((ctx, error) => stream.errorStream().emit(ctx, error));
            let handlerContext = context;
            let state;
            let error;
            let cancelled = false;
            try {
                try {
                    const begun = await handler.beginRequest(context, stream);
                    state = begun.state;
                    if (begun.context === undefined || begun.context === null) {
                        error = begun.error ?? new Error("Query beginRequest returned no context");
                    }
                    else {
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
                        }
                        catch (cause) {
                            cancelled = operationContext.signal().aborted;
                            throw cause;
                        }
                        finally {
                            // Uses the existing context timer/listener cleanup. Never race the
                            // operation promise: its native cleanup completes before endRequest.
                            scope?.abort();
                        }
                    }
                }
                catch (cause) {
                    error = errorFromUnknown(cause);
                }
                await handler.endRequest(handlerContext, stream, state, error, out, errors);
            }
            finally {
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
//# sourceMappingURL=index.js.map