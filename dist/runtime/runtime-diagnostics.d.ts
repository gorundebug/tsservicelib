import { Context } from "./context.js";
import type { Metrics } from "./environment/index.js";
import type { Lifecycle } from "./lifecycle.js";
import type { PriorityTaskPool, TaskPool } from "./pool/index.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";
/**
 * Node-specific runtime diagnostics. Sampling exists only when metrics are
 * enabled, so benchmark builds using NoopMetricsEngine keep the zero-work
 * telemetry path.
 */
export declare class RuntimeDiagnostics implements Lifecycle {
    #private;
    constructor(metrics: Metrics, service: string, tasks: RuntimeTaskRegistry, taskPools: readonly TaskPool[], priorityTaskPools: readonly PriorityTaskPool[]);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private eventLoopLagSeconds;
    private sampleEventLoopUtilization;
}
//# sourceMappingURL=runtime-diagnostics.d.ts.map