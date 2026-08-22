import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

import type { Context } from "../../context.js";
import type {
  Float64Histogram,
  Float64HistogramVec,
  Int64Counter,
  Int64CounterVec,
  Int64Gauge,
  Int64GaugeVec,
  Labels,
  Metrics,
  MetricsEngine,
  MetricsScope
} from "./metrics.js";

type Family =
  | {
      readonly kind: "counter";
      readonly help: string;
      readonly labelNames: readonly string[];
      readonly metric: Counter;
    }
  | {
      readonly kind: "gauge";
      readonly help: string;
      readonly labelNames: readonly string[];
      readonly metric: Gauge;
    }
  | {
      readonly kind: "histogram";
      readonly help: string;
      readonly labelNames: readonly string[];
      readonly buckets: readonly number[] | undefined;
      readonly metric: Histogram;
    }
  | {
      readonly kind: "observable-gauge";
      readonly help: string;
      readonly labelNames: readonly string[];
      readonly observers: Map<string, { readonly labels: Labels; readonly observe: () => number }>;
      readonly metric: Gauge;
    };

export class PrometheusMetrics implements Metrics {
  readonly #registry: Registry;
  readonly #families = new Map<string, Family>();

  public constructor(registry: Registry = new Registry()) {
    this.#registry = registry;
    // Use prom-client's standard Node.js/process collectors instead of
    // inventing framework-specific heap, process and V8 runtime metrics.
    // PrometheusMetrics is instantiated only when runtime telemetry is enabled;
    // stripped benchmark/profiling builds keep the zero-work Noop engine.
    collectDefaultMetrics({ register: this.#registry });
  }

  public enabled(): boolean {
    return true;
  }

  public scope(prefix: string, labels: Labels = {}): MetricsScope {
    return new PrometheusMetricsScope(this, prefix, labels);
  }

  public registry(): Registry {
    return this.#registry;
  }

  public counter(name: string, help: string, labels: Labels): Int64Counter {
    const labelNames = sortedLabelNames(labels);
    const existing = this.#families.get(name);
    let family: Extract<Family, { readonly kind: "counter" }>;
    if (existing === undefined) {
      family = {
        kind: "counter",
        help,
        labelNames,
        metric: new Counter({
          name,
          help,
          labelNames: [...labelNames],
          registers: [this.#registry]
        })
      };
      this.#families.set(name, family);
    } else {
      requireFamily(existing, "counter", name, help, labelNames);
      family = existing;
    }
    return new PrometheusCounter(family.metric, labels);
  }

  public gauge(name: string, help: string, labels: Labels): Int64Gauge {
    const labelNames = sortedLabelNames(labels);
    const existing = this.#families.get(name);
    let family: Extract<Family, { readonly kind: "gauge" }>;
    if (existing === undefined) {
      family = {
        kind: "gauge",
        help,
        labelNames,
        metric: new Gauge({ name, help, labelNames: [...labelNames], registers: [this.#registry] })
      };
      this.#families.set(name, family);
    } else {
      requireFamily(existing, "gauge", name, help, labelNames);
      family = existing;
    }
    return new PrometheusGauge(family.metric, labels);
  }

  public deleteGauge(name: string, help: string, labels: Labels): void {
    const existing = this.#families.get(name);
    if (existing === undefined) {
      return;
    }
    requireFamily(existing, "gauge", name, help, sortedLabelNames(labels));
    existing.metric.remove(labels);
  }

  public histogram(
    name: string,
    help: string,
    labels: Labels,
    buckets?: readonly number[]
  ): Float64Histogram {
    const labelNames = sortedLabelNames(labels);
    const existing = this.#families.get(name);
    let family: Extract<Family, { readonly kind: "histogram" }>;
    if (existing === undefined) {
      const options = {
        name,
        help,
        labelNames: [...labelNames],
        registers: [this.#registry],
        ...(buckets === undefined ? {} : { buckets: [...buckets] })
      };
      family = {
        kind: "histogram",
        help,
        labelNames,
        buckets,
        metric: new Histogram(options)
      };
      this.#families.set(name, family);
    } else {
      requireFamily(existing, "histogram", name, help, labelNames);
      if (!sameNumbers(existing.buckets, buckets)) {
        throw new Error(`metric ${name} is already registered with different buckets`);
      }
      family = existing;
    }
    return new PrometheusHistogram(family.metric, labels);
  }

  public observableGauge(name: string, help: string, labels: Labels, observe: () => number): void {
    const labelNames = sortedLabelNames(labels);
    const existing = this.#families.get(name);
    let family: Extract<Family, { readonly kind: "observable-gauge" }>;
    if (existing === undefined) {
      const observers = new Map<
        string,
        { readonly labels: Labels; readonly observe: () => number }
      >();
      const metric = new Gauge({
        name,
        help,
        labelNames: [...labelNames],
        registers: [this.#registry],
        collect() {
          for (const observer of observers.values()) {
            this.set(observer.labels, observer.observe());
          }
        }
      });
      family = { kind: "observable-gauge", help, labelNames, observers, metric };
      this.#families.set(name, family);
    } else {
      requireFamily(existing, "observable-gauge", name, help, labelNames);
      family = existing;
    }
    const key = labelsKey(labels);
    if (family.observers.has(key)) {
      throw new Error(`metric ${name} already has an observer for labels ${key}`);
    }
    family.observers.set(key, { labels: { ...labels }, observe });
  }
}

export class PrometheusMetricsEngine implements MetricsEngine {
  readonly #metrics: PrometheusMetrics;

  public constructor(metrics: PrometheusMetrics = new PrometheusMetrics()) {
    this.#metrics = metrics;
  }

  public metrics(): PrometheusMetrics {
    return this.#metrics;
  }

  public contentType(): string {
    return this.#metrics.registry().contentType;
  }

  public render(): Promise<string> {
    return this.#metrics.registry().metrics();
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }
}

class PrometheusMetricsScope implements MetricsScope {
  readonly #metrics: PrometheusMetrics;
  readonly #prefix: string;
  readonly #labels: Labels;

  public constructor(metrics: PrometheusMetrics, prefix: string, labels: Labels) {
    this.#metrics = metrics;
    this.#prefix = prefix;
    this.#labels = { ...labels };
  }

  public counter(name: string, help: string, labels: Labels = {}): Int64Counter {
    return this.#metrics.counter(
      metricName(this.#prefix, name),
      help,
      mergeLabels(this.#labels, labels)
    );
  }

  public counterVec(name: string, help: string): Int64CounterVec {
    return new PrometheusCounterVec(
      this.#metrics,
      metricName(this.#prefix, name),
      help,
      this.#labels
    );
  }

  public gauge(name: string, help: string, labels: Labels = {}): Int64Gauge {
    return this.#metrics.gauge(
      metricName(this.#prefix, name),
      help,
      mergeLabels(this.#labels, labels)
    );
  }

  public gaugeVec(name: string, help: string): Int64GaugeVec {
    return new PrometheusGaugeVec(
      this.#metrics,
      metricName(this.#prefix, name),
      help,
      this.#labels
    );
  }

  public histogram(
    name: string,
    help: string,
    labels: Labels = {},
    buckets?: readonly number[]
  ): Float64Histogram {
    return this.#metrics.histogram(
      metricName(this.#prefix, name),
      help,
      mergeLabels(this.#labels, labels),
      buckets
    );
  }

  public histogramVec(
    name: string,
    help: string,
    buckets?: readonly number[]
  ): Float64HistogramVec {
    return new PrometheusHistogramVec(
      this.#metrics,
      metricName(this.#prefix, name),
      help,
      this.#labels,
      buckets
    );
  }

  public observableFloat64Gauge(name: string, help: string, observe: () => number): void {
    this.#metrics.observableGauge(metricName(this.#prefix, name), help, this.#labels, observe);
  }
}

class PrometheusCounter implements Int64Counter {
  readonly #metric: Counter;
  readonly #labels: Labels;

  public constructor(metric: Counter, labels: Labels) {
    this.#metric = metric;
    this.#labels = labels;
  }

  public inc(context: Context): void {
    this.add(context, 1);
  }

  public add(context: Context, value: number): void {
    void context;
    requireSafeInteger(value);
    if (value < 0) {
      throw new RangeError("counter cannot decrease");
    }
    this.#metric.inc(this.#labels, value);
  }
}

class PrometheusGauge implements Int64Gauge {
  readonly #metric: Gauge;
  readonly #labels: Labels;

  public constructor(metric: Gauge, labels: Labels) {
    this.#metric = metric;
    this.#labels = labels;
  }

  public set(value: number): void {
    requireSafeInteger(value);
    this.#metric.set(this.#labels, value);
  }

  public inc(): void {
    this.#metric.inc(this.#labels);
  }

  public dec(): void {
    this.#metric.dec(this.#labels);
  }

  public add(delta: number): void {
    requireSafeInteger(delta);
    this.#metric.inc(this.#labels, delta);
  }

  public sub(delta: number): void {
    requireSafeInteger(delta);
    this.#metric.dec(this.#labels, delta);
  }
}

class PrometheusHistogram implements Float64Histogram {
  readonly #metric: Histogram;
  readonly #labels: Labels;

  public constructor(metric: Histogram, labels: Labels) {
    this.#metric = metric;
    this.#labels = labels;
  }

  public observe(context: Context, value: number): void {
    void context;
    if (!Number.isFinite(value)) {
      throw new RangeError("histogram observation must be finite");
    }
    this.#metric.observe(this.#labels, value);
  }
}

class PrometheusCounterVec implements Int64CounterVec {
  readonly #metrics: PrometheusMetrics;
  readonly #name: string;
  readonly #help: string;
  readonly #labels: Labels;

  public constructor(metrics: PrometheusMetrics, name: string, help: string, labels: Labels) {
    this.#metrics = metrics;
    this.#name = name;
    this.#help = help;
    this.#labels = labels;
  }

  public with(labels: Labels): Int64Counter {
    return this.#metrics.counter(this.#name, this.#help, mergeLabels(this.#labels, labels));
  }
}

class PrometheusGaugeVec implements Int64GaugeVec {
  readonly #metrics: PrometheusMetrics;
  readonly #name: string;
  readonly #help: string;
  readonly #labels: Labels;

  public constructor(metrics: PrometheusMetrics, name: string, help: string, labels: Labels) {
    this.#metrics = metrics;
    this.#name = name;
    this.#help = help;
    this.#labels = labels;
  }

  public with(labels: Labels): Int64Gauge {
    return this.#metrics.gauge(this.#name, this.#help, mergeLabels(this.#labels, labels));
  }

  public delete(labels: Labels): void {
    this.#metrics.deleteGauge(this.#name, this.#help, mergeLabels(this.#labels, labels));
  }
}

class PrometheusHistogramVec implements Float64HistogramVec {
  readonly #metrics: PrometheusMetrics;
  readonly #name: string;
  readonly #help: string;
  readonly #labels: Labels;
  readonly #buckets: readonly number[] | undefined;

  public constructor(
    metrics: PrometheusMetrics,
    name: string,
    help: string,
    labels: Labels,
    buckets: readonly number[] | undefined
  ) {
    this.#metrics = metrics;
    this.#name = name;
    this.#help = help;
    this.#labels = labels;
    this.#buckets = buckets;
  }

  public with(labels: Labels): Float64Histogram {
    return this.#metrics.histogram(
      this.#name,
      this.#help,
      mergeLabels(this.#labels, labels),
      this.#buckets
    );
  }
}

function requireFamily<K extends Family["kind"]>(
  family: Family,
  kind: K,
  name: string,
  help: string,
  labelNames: readonly string[]
): asserts family is Extract<Family, { readonly kind: K }> {
  if (family.kind !== kind) {
    throw new Error(`metric ${name} is already registered as ${family.kind}`);
  }
  if (family.help !== help) {
    throw new Error(`metric ${name} is already registered with different help`);
  }
  if (!sameStrings(family.labelNames, labelNames)) {
    throw new Error(`metric ${name} is already registered with different label names`);
  }
}

function metricName(prefix: string, name: string): string {
  return prefix.length === 0 ? name : `${prefix}_${name}`;
}

function mergeLabels(base: Labels, extra: Labels): Labels {
  return { ...base, ...extra };
}

function sortedLabelNames(labels: Labels): readonly string[] {
  return Object.keys(labels).sort();
}

function labelsKey(labels: Labels): string {
  return Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
    .join(",");
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function sameNumbers(
  left: readonly number[] | undefined,
  right: readonly number[] | undefined
): boolean {
  if (left === undefined || right === undefined) {
    return left === right;
  }
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function requireSafeInteger(value: number): void {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError("integer metric value must be a safe integer");
  }
}
