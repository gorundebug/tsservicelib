import type { Context } from "../context.js";
import type { Attribute, SpanStatusCode, Tracer, Tracing, TracingEngine } from "../environment/index.js";
export interface RecordedEvent {
    readonly name: string;
    readonly attributes: readonly Attribute[];
}
export interface RecordedSpan {
    readonly tracerName: string;
    readonly name: string;
    readonly attributes: readonly Attribute[];
    readonly events: readonly RecordedEvent[];
    readonly statusCode: SpanStatusCode;
    readonly statusDescription: string;
    readonly error: Error | undefined;
}
export declare class TestTracing implements Tracing, TracingEngine {
    #private;
    enabled(): boolean;
    tracing(): Tracing;
    tracer(name: string): Tracer;
    spans(): readonly RecordedSpan[];
    reset(): void;
    shutdown(context: Context): Promise<void>;
    record(span: RecordedSpan): void;
}
//# sourceMappingURL=index.d.ts.map