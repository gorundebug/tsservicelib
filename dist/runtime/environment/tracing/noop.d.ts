import type { Context } from "../../context.js";
import type { Span, Tracer, Tracing, TracingEngine } from "./tracing.js";
export declare class NoopTracingEngine implements TracingEngine {
    tracing(): Tracing;
    shutdown(context: Context): Promise<void>;
}
export declare const noopSpan: Span;
export declare const noopTracer: Tracer;
export declare const noopTracing: Tracing;
export declare const noopTracingEngine: TracingEngine;
//# sourceMappingURL=noop.d.ts.map