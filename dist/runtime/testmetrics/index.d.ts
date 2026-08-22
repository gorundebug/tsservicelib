import type { Context } from "../context.js";
import type { Float64Histogram, Int64Counter, Int64Gauge, Labels, Metrics, MetricsScope } from "../environment/index.js";
interface NumberSeries {
    value: number;
}
export interface HistogramSnapshot {
    readonly count: number;
    readonly sum: number;
    readonly values: readonly number[];
}
export declare class TestMetrics implements Metrics {
    #private;
    enabled(): boolean;
    scope(prefix: string, labels?: Labels): MetricsScope;
    registeredNames(): readonly string[];
    counterValue(name: string, labels?: Labels): number | undefined;
    gaugeValue(name: string, labels?: Labels): number | undefined;
    histogramValue(name: string, labels?: Labels): HistogramSnapshot | undefined;
    observableGaugeValue(name: string, labels?: Labels): number | undefined;
    deleteNumberInstrument(name: string, labels: Labels): void;
    numberInstrument(kind: "counter" | "gauge", name: string, help: string, labels: Labels): TestNumberInstrument;
    histogramInstrument(name: string, help: string, labels: Labels): Float64Histogram;
    observableInstrument(name: string, help: string, labels: Labels, observe: () => number): void;
    private registerFamily;
}
declare class TestNumberInstrument implements Int64Counter, Int64Gauge {
    #private;
    constructor(kind: "counter" | "gauge", series: NumberSeries);
    inc(context?: Context): void;
    dec(): void;
    add(contextOrDelta: Context | number, value?: number): void;
    sub(delta: number): void;
    set(value: number): void;
    private addValue;
}
export {};
//# sourceMappingURL=index.d.ts.map