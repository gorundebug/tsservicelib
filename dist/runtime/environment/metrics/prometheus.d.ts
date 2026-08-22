import { Registry } from "prom-client";
import type { Context } from "../../context.js";
import type { Float64Histogram, Int64Counter, Int64Gauge, Labels, Metrics, MetricsEngine, MetricsScope } from "./metrics.js";
export declare class PrometheusMetrics implements Metrics {
    #private;
    constructor(registry?: Registry);
    enabled(): boolean;
    scope(prefix: string, labels?: Labels): MetricsScope;
    registry(): Registry;
    counter(name: string, help: string, labels: Labels): Int64Counter;
    gauge(name: string, help: string, labels: Labels): Int64Gauge;
    deleteGauge(name: string, help: string, labels: Labels): void;
    histogram(name: string, help: string, labels: Labels, buckets?: readonly number[]): Float64Histogram;
    observableGauge(name: string, help: string, labels: Labels, observe: () => number): void;
}
export declare class PrometheusMetricsEngine implements MetricsEngine {
    #private;
    constructor(metrics?: PrometheusMetrics);
    metrics(): PrometheusMetrics;
    contentType(): string;
    render(): Promise<string>;
    shutdown(context: Context): Promise<void>;
}
//# sourceMappingURL=prometheus.d.ts.map