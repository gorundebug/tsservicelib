import type { Context } from "../../context.js";

export type Labels = Readonly<Record<string, string>>;

export interface Int64Counter {
  inc(context: Context): void;
  add(context: Context, value: number): void;
}

export interface Int64CounterVec {
  with(labels: Labels): Int64Counter;
}

export interface Float64Counter {
  add(context: Context, value: number): void;
}

export interface Float64CounterVec {
  with(labels: Labels): Float64Counter;
}

export interface Float64Gauge {
  set(value: number): void;
  inc(): void;
  dec(): void;
  add(delta: number): void;
  sub(delta: number): void;
}

export interface Float64GaugeVec {
  with(labels: Labels): Float64Gauge;
  delete(labels: Labels): void;
}

export interface Int64Gauge {
  set(value: number): void;
  inc(): void;
  dec(): void;
  add(delta: number): void;
  sub(delta: number): void;
}

export interface Int64GaugeVec {
  with(labels: Labels): Int64Gauge;
  delete(labels: Labels): void;
}

export interface Float64Histogram {
  observe(context: Context, value: number): void;
}

export interface Float64HistogramVec {
  with(labels: Labels): Float64Histogram;
}

export interface Int64Histogram {
  observe(context: Context, value: number): void;
}

export interface Int64HistogramVec {
  with(labels: Labels): Int64Histogram;
}

export interface MetricsScope {
  counter(name: string, help: string, labels?: Labels): Int64Counter;
  counterVec(name: string, help: string): Int64CounterVec;
  gauge(name: string, help: string, labels?: Labels): Int64Gauge;
  gaugeVec(name: string, help: string): Int64GaugeVec;
  histogram(
    name: string,
    help: string,
    labels?: Labels,
    buckets?: readonly number[]
  ): Float64Histogram;
  histogramVec(name: string, help: string, buckets?: readonly number[]): Float64HistogramVec;
  observableFloat64Gauge(name: string, help: string, observe: () => number): void;
}

export interface Metrics {
  enabled(): boolean;
  scope(prefix: string, labels?: Labels): MetricsScope;
}

export interface MetricsEngine {
  metrics(): Metrics;
  shutdown(context: Context): Promise<void>;
}
