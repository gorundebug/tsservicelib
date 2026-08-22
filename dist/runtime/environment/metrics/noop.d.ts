import type { Context } from "../../context.js";
import type { Metrics, MetricsEngine } from "./metrics.js";
export declare class NoopMetricsEngine implements MetricsEngine {
    metrics(): Metrics;
    shutdown(context: Context): Promise<void>;
    contentType(): string;
    render(): Promise<string>;
}
export declare const noopMetrics: Metrics;
export declare const noopMetricsEngine: MetricsEngine;
//# sourceMappingURL=noop.d.ts.map