import type { Context } from "../context.js";
import type {
  Float64Histogram,
  Float64HistogramVec,
  Int64Counter,
  Int64CounterVec,
  Int64Gauge,
  Int64GaugeVec,
  Labels,
  Metrics,
  MetricsScope
} from "../environment/index.js";

type MetricKind = "counter" | "gauge" | "histogram" | "observable-gauge";

interface Family {
  readonly kind: MetricKind;
  readonly help: string;
}

interface NumberSeries {
  value: number;
}

interface HistogramSeries {
  readonly values: number[];
}

interface ObservableSeries {
  readonly observe: () => number;
}

export interface HistogramSnapshot {
  readonly count: number;
  readonly sum: number;
  readonly values: readonly number[];
}

export class TestMetrics implements Metrics {
  readonly #families = new Map<string, Family>();
  readonly #numbers = new Map<string, NumberSeries>();
  readonly #histograms = new Map<string, HistogramSeries>();
  readonly #observables = new Map<string, ObservableSeries>();

  public enabled(): boolean {
    return true;
  }

  public scope(prefix: string, labels: Labels = {}): MetricsScope {
    return new TestMetricsScope(this, prefix, labels);
  }

  public registeredNames(): readonly string[] {
    return [...this.#families.keys()].sort();
  }

  public counterValue(name: string, labels: Labels = {}): number | undefined {
    return this.#numbers.get(seriesKey(name, labels))?.value;
  }

  public gaugeValue(name: string, labels: Labels = {}): number | undefined {
    return this.#numbers.get(seriesKey(name, labels))?.value;
  }

  public histogramValue(name: string, labels: Labels = {}): HistogramSnapshot | undefined {
    const values = this.#histograms.get(seriesKey(name, labels))?.values;
    if (values === undefined) {
      return undefined;
    }
    return {
      count: values.length,
      sum: values.reduce((sum, value) => sum + value, 0),
      values: [...values]
    };
  }

  public observableGaugeValue(name: string, labels: Labels = {}): number | undefined {
    return this.#observables.get(seriesKey(name, labels))?.observe();
  }

  public deleteNumberInstrument(name: string, labels: Labels): void {
    this.#numbers.delete(seriesKey(name, labels));
  }

  public numberInstrument(
    kind: "counter" | "gauge",
    name: string,
    help: string,
    labels: Labels
  ): TestNumberInstrument {
    this.registerFamily(name, kind, help);
    const key = seriesKey(name, labels);
    let series = this.#numbers.get(key);
    if (series === undefined) {
      series = { value: 0 };
      this.#numbers.set(key, series);
    }
    return new TestNumberInstrument(kind, series);
  }

  public histogramInstrument(name: string, help: string, labels: Labels): Float64Histogram {
    this.registerFamily(name, "histogram", help);
    const key = seriesKey(name, labels);
    let series = this.#histograms.get(key);
    if (series === undefined) {
      series = { values: [] };
      this.#histograms.set(key, series);
    }
    return new TestHistogram(series);
  }

  public observableInstrument(
    name: string,
    help: string,
    labels: Labels,
    observe: () => number
  ): void {
    this.registerFamily(name, "observable-gauge", help);
    const key = seriesKey(name, labels);
    if (this.#observables.has(key)) {
      throw new Error(`metric series ${key} is already registered`);
    }
    this.#observables.set(key, { observe });
  }

  private registerFamily(name: string, kind: MetricKind, help: string): void {
    const existing = this.#families.get(name);
    if (existing === undefined) {
      this.#families.set(name, { kind, help });
      return;
    }
    if (existing.kind !== kind) {
      throw new Error(`metric ${name} is already registered as ${existing.kind}`);
    }
    if (existing.help !== help) {
      throw new Error(`metric ${name} is already registered with different help`);
    }
  }
}

class TestMetricsScope implements MetricsScope {
  readonly #metrics: TestMetrics;
  readonly #prefix: string;
  readonly #labels: Labels;

  public constructor(metrics: TestMetrics, prefix: string, labels: Labels) {
    this.#metrics = metrics;
    this.#prefix = prefix;
    this.#labels = { ...labels };
  }

  public counter(name: string, help: string, labels: Labels = {}): Int64Counter {
    return this.#metrics.numberInstrument(
      "counter",
      metricName(this.#prefix, name),
      help,
      mergeLabels(this.#labels, labels)
    );
  }

  public counterVec(name: string, help: string): Int64CounterVec {
    return new TestCounterVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
  }

  public gauge(name: string, help: string, labels: Labels = {}): Int64Gauge {
    return this.#metrics.numberInstrument(
      "gauge",
      metricName(this.#prefix, name),
      help,
      mergeLabels(this.#labels, labels)
    );
  }

  public gaugeVec(name: string, help: string): Int64GaugeVec {
    return new TestGaugeVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
  }

  public histogram(
    name: string,
    help: string,
    labels: Labels = {},
    buckets?: readonly number[]
  ): Float64Histogram {
    void buckets;
    return this.#metrics.histogramInstrument(
      metricName(this.#prefix, name),
      help,
      mergeLabels(this.#labels, labels)
    );
  }

  public histogramVec(
    name: string,
    help: string,
    buckets?: readonly number[]
  ): Float64HistogramVec {
    void buckets;
    return new TestHistogramVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
  }

  public observableFloat64Gauge(name: string, help: string, observe: () => number): void {
    this.#metrics.observableInstrument(metricName(this.#prefix, name), help, this.#labels, observe);
  }
}

class TestNumberInstrument implements Int64Counter, Int64Gauge {
  readonly #kind: "counter" | "gauge";
  readonly #series: NumberSeries;

  public constructor(kind: "counter" | "gauge", series: NumberSeries) {
    this.#kind = kind;
    this.#series = series;
  }

  public inc(context?: Context): void {
    void context;
    this.addValue(1);
  }

  public dec(): void {
    this.addValue(-1);
  }

  public add(contextOrDelta: Context | number, value?: number): void {
    const delta = typeof contextOrDelta === "number" ? contextOrDelta : value;
    if (delta === undefined) {
      throw new Error("counter add value is missing");
    }
    this.addValue(delta);
  }

  public sub(delta: number): void {
    this.addValue(-delta);
  }

  public set(value: number): void {
    if (this.#kind !== "gauge") {
      throw new Error("counter cannot be set");
    }
    requireFiniteSafeInteger(value);
    this.#series.value = value;
  }

  private addValue(delta: number): void {
    requireFiniteSafeInteger(delta);
    if (this.#kind === "counter" && delta < 0) {
      throw new RangeError("counter cannot decrease");
    }
    this.#series.value += delta;
  }
}

class TestHistogram implements Float64Histogram {
  readonly #series: HistogramSeries;

  public constructor(series: HistogramSeries) {
    this.#series = series;
  }

  public observe(context: Context, value: number): void {
    void context;
    if (!Number.isFinite(value)) {
      throw new RangeError("histogram observation must be finite");
    }
    this.#series.values.push(value);
  }
}

class TestCounterVec implements Int64CounterVec {
  readonly #metrics: TestMetrics;
  readonly #name: string;
  readonly #help: string;
  readonly #labels: Labels;

  public constructor(metrics: TestMetrics, name: string, help: string, labels: Labels) {
    this.#metrics = metrics;
    this.#name = name;
    this.#help = help;
    this.#labels = labels;
  }

  public with(labels: Labels): Int64Counter {
    return this.#metrics.numberInstrument(
      "counter",
      this.#name,
      this.#help,
      mergeLabels(this.#labels, labels)
    );
  }
}

class TestGaugeVec implements Int64GaugeVec {
  readonly #metrics: TestMetrics;
  readonly #name: string;
  readonly #help: string;
  readonly #labels: Labels;

  public constructor(metrics: TestMetrics, name: string, help: string, labels: Labels) {
    this.#metrics = metrics;
    this.#name = name;
    this.#help = help;
    this.#labels = labels;
  }

  public with(labels: Labels): Int64Gauge {
    return this.#metrics.numberInstrument(
      "gauge",
      this.#name,
      this.#help,
      mergeLabels(this.#labels, labels)
    );
  }

  public delete(labels: Labels): void {
    this.#metrics.deleteNumberInstrument(this.#name, mergeLabels(this.#labels, labels));
  }
}

class TestHistogramVec implements Float64HistogramVec {
  readonly #metrics: TestMetrics;
  readonly #name: string;
  readonly #help: string;
  readonly #labels: Labels;

  public constructor(metrics: TestMetrics, name: string, help: string, labels: Labels) {
    this.#metrics = metrics;
    this.#name = name;
    this.#help = help;
    this.#labels = labels;
  }

  public with(labels: Labels): Float64Histogram {
    return this.#metrics.histogramInstrument(
      this.#name,
      this.#help,
      mergeLabels(this.#labels, labels)
    );
  }
}

function metricName(prefix: string, name: string): string {
  return prefix.length === 0 ? name : `${prefix}_${name}`;
}

function mergeLabels(base: Labels, extra: Labels): Labels {
  return { ...base, ...extra };
}

function seriesKey(name: string, labels: Labels): string {
  const suffix = Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
    .join(",");
  return `${name}{${suffix}}`;
}

function requireFiniteSafeInteger(value: number): void {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError("integer metric value must be a safe integer");
  }
}
