import type { Context } from "../runtime/context.js";
import type { Metrics } from "../runtime/environment/metrics/index.js";
/** Native observations; no settlement or business-completion inference. */
export declare class SourceMetrics {
    #private;
    readonly enabled: boolean;
    constructor(metrics: Metrics, protocol: string, connector: string, endpoint: string);
    observe(context: Context, observation: string): void;
}
//# sourceMappingURL=broker-source-metrics.d.ts.map