import type { RuntimeEnvironment } from "../environment/index.js";
import type { MetricsEngine } from "../environment/metrics/index.js";
export declare function registerRuntimeHTTPHandlers(environment: RuntimeEnvironment, metricsEngine: MetricsEngine & {
    contentType(): string;
    render(): Promise<string>;
}): void;
//# sourceMappingURL=http.d.ts.map