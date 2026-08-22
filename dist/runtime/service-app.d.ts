import { type CanonicalConfig, type RuntimeConfigReloadSource, type RuntimeConfigStore } from "./config/index.js";
import { Context } from "./context.js";
import { type Logger, type LogsEngine, NoopMetricsEngine, PrometheusMetricsEngine, type Tracing, type TracingEngine } from "./environment/index.js";
import { ServiceEnvironment, type JoinStorageFactory } from "./environment/runtime-environment.js";
import { DelayPool } from "./pool/index.js";
import { ServiceRuntime } from "./service-runtime.js";
import { type SerdeRegistry } from "./serde/index.js";
import type { CallerFactory } from "./stream.js";
export interface ServiceAppOptions<T extends CanonicalConfig = CanonicalConfig> {
    readonly callerFactory?: CallerFactory | undefined;
    readonly delayPool?: DelayPool | undefined;
    readonly serdeRegistry?: SerdeRegistry | undefined;
    readonly logger?: Logger | undefined;
    readonly logsEngine?: LogsEngine | undefined;
    readonly metricsEngine?: ServiceMetricsEngine | undefined;
    readonly tracing?: Tracing | undefined;
    readonly tracingEngine?: TracingEngine | undefined;
    readonly configReload?: RuntimeConfigReloadSource<T> | undefined;
    readonly joinStorageFactory?: JoinStorageFactory | undefined;
}
/** Owns one generated service runtime and its canonical component lifecycle. */
export declare class ServiceApp<T extends CanonicalConfig = CanonicalConfig> {
    #private;
    constructor(config: RuntimeConfigStore<T>, serviceId: number, options?: ServiceAppOptions<T>);
    environment(): ServiceEnvironment<T>;
    runtime(): ServiceRuntime;
    metricsEngine(): ServiceMetricsEngine;
    start(context?: Context): Promise<void>;
    stop(context?: Context, drainTimeoutMs?: number): Promise<void>;
    private prepare;
}
export type ServiceMetricsEngine = (PrometheusMetricsEngine | NoopMetricsEngine) & {
    contentType(): string;
    render(): Promise<string>;
};
export declare function makeServiceMetricsEngine(environment?: NodeJS.ProcessEnv): ServiceMetricsEngine;
export declare function makeServiceLogsEngine(environment?: NodeJS.ProcessEnv): LogsEngine;
//# sourceMappingURL=service-app.d.ts.map