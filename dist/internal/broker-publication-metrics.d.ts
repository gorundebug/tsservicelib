import type { Context } from "../runtime/context.js";
import type { Metrics } from "../runtime/environment/metrics/index.js";
/** Fixed transport outcomes; never derived from payloads, IDs or error strings. */
export declare class PublicationMetrics {
    #private;
    constructor(metrics: Metrics, protocol: string, connector: string, endpoint: string);
    observe(context: Context, outcome: string): void;
}
//# sourceMappingURL=broker-publication-metrics.d.ts.map