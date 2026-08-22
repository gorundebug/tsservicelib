import type { Context } from "../../context.js";
import type {
  Float64Counter,
  Float64CounterVec,
  Float64Gauge,
  Float64GaugeVec,
  Float64Histogram,
  Float64HistogramVec,
  Int64Counter,
  Int64CounterVec,
  Int64Gauge,
  Int64GaugeVec,
  Int64Histogram,
  Int64HistogramVec,
  Labels,
  Metrics,
  MetricsEngine,
  MetricsScope
} from "./metrics.js";

class NoopInstrument
  implements
    Int64Counter,
    Float64Counter,
    Int64Gauge,
    Float64Gauge,
    Int64Histogram,
    Float64Histogram
{
  public inc(): void {
    return;
  }
  public dec(): void {
    return;
  }
  public add(): void {
    return;
  }
  public sub(): void {
    return;
  }
  public set(): void {
    return;
  }
  public observe(): void {
    return;
  }
}

class NoopVector
  implements
    Int64CounterVec,
    Float64CounterVec,
    Int64GaugeVec,
    Float64GaugeVec,
    Int64HistogramVec,
    Float64HistogramVec
{
  public with(labels: Labels): NoopInstrument {
    void labels;
    return noopInstrument;
  }

  public delete(labels: Labels): void {
    void labels;
  }
}

class NoopMetricsScope implements MetricsScope {
  public counter(name: string, help: string, labels?: Labels): Int64Counter {
    void name;
    void help;
    void labels;
    return noopInstrument;
  }

  public counterVec(name: string, help: string): Int64CounterVec {
    void name;
    void help;
    return noopVector;
  }

  public gauge(name: string, help: string, labels?: Labels): Int64Gauge {
    void name;
    void help;
    void labels;
    return noopInstrument;
  }

  public gaugeVec(name: string, help: string): Int64GaugeVec {
    void name;
    void help;
    return noopVector;
  }

  public histogram(
    name: string,
    help: string,
    labels?: Labels,
    buckets?: readonly number[]
  ): Float64Histogram {
    void name;
    void help;
    void labels;
    void buckets;
    return noopInstrument;
  }

  public histogramVec(
    name: string,
    help: string,
    buckets?: readonly number[]
  ): Float64HistogramVec {
    void name;
    void help;
    void buckets;
    return noopVector;
  }

  public observableFloat64Gauge(name: string, help: string, observe: () => number): void {
    void name;
    void help;
    void observe;
  }
}

class NoopMetrics implements Metrics {
  public enabled(): boolean {
    return false;
  }

  public scope(prefix: string, labels?: Labels): MetricsScope {
    void prefix;
    void labels;
    return noopScope;
  }
}

export class NoopMetricsEngine implements MetricsEngine {
  public metrics(): Metrics {
    return noopMetrics;
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }

  public contentType(): string {
    return "text/plain; version=0.0.4; charset=utf-8";
  }

  public render(): Promise<string> {
    return Promise.resolve("# ServiceLib runtime metrics are disabled.\n");
  }
}

const noopInstrument = new NoopInstrument();
const noopVector = new NoopVector();
const noopScope = new NoopMetricsScope();
export const noopMetrics: Metrics = new NoopMetrics();
export const noopMetricsEngine: MetricsEngine = new NoopMetricsEngine();
