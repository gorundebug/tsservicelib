import { type BufferConfig, type SpanExporter } from "@opentelemetry/sdk-trace-node";
import type { Context } from "../../context.js";
import { type Tracing, type TracingEngine } from "../../environment/index.js";
export interface OpenTelemetryTracingOptions {
    readonly serviceName: string;
    readonly endpoint?: string;
    readonly exportTimeoutMillis?: number;
    readonly exporter?: SpanExporter;
    readonly batch?: BufferConfig;
    readonly resourceAttributes?: Readonly<Record<string, string | number | boolean>>;
}
export declare class OpenTelemetryTracingEngine implements TracingEngine {
    #private;
    constructor(options: OpenTelemetryTracingOptions);
    tracing(): Tracing;
    shutdown(context: Context): Promise<void>;
}
//# sourceMappingURL=tracing.d.ts.map