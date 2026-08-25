import type { Logger } from "../../runtime/environment/log.js";
import type { Labels, Metrics, MetricsScope } from "../../runtime/environment/metrics/metrics.js";
import { type Tracer, type Tracing } from "../../runtime/environment/tracing/tracing.js";
export declare const workflowLogger: Logger;
export declare class WorkflowMetrics implements Metrics {
    enabled(): boolean;
    scope(prefix: string, labels?: Labels): MetricsScope;
}
export declare class WorkflowTracing implements Tracing {
    enabled(): boolean;
    tracer(name: string): Tracer;
}
//# sourceMappingURL=workflow-telemetry.d.ts.map