import {
  context as activeContext,
  SpanStatusCode as OtelSpanStatusCode,
  trace,
  type Attributes,
  type Span as OtelSpan
} from "@opentelemetry/api";
import { log, metricMeter } from "@temporalio/workflow";

import type { Context, MessageContext } from "../../runtime/context.js";
import type { LogField, Logger } from "../../runtime/environment/log.js";
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
} from "../../runtime/environment/metrics/metrics.js";
import {
  SpanStatusCode,
  type Attribute,
  type Span,
  type SpanContext,
  type StartedSpan,
  type Tracer,
  type Tracing
} from "../../runtime/environment/tracing/tracing.js";

export const workflowLogger: Logger = {
  debug: (_context, message, ...fields) => {
    log.debug(message, logFields(fields));
  },
  info: (_context, message, ...fields) => {
    log.info(message, logFields(fields));
  },
  warn: (_context, message, ...fields) => {
    log.warn(message, logFields(fields));
  },
  error: (_context, message, ...fields) => {
    log.error(message, logFields(fields));
  }
};

export class WorkflowMetrics implements Metrics {
  public enabled(): boolean {
    return true;
  }

  public scope(prefix: string, labels: Labels = {}): MetricsScope {
    return new WorkflowMetricsScope(prefix, labels);
  }
}

class WorkflowMetricsScope implements MetricsScope {
  readonly #prefix: string;
  readonly #labels: Labels;

  public constructor(prefix: string, labels: Labels) {
    this.#prefix = prefix;
    this.#labels = labels;
  }

  public counter(name: string, help: string, labels: Labels = {}): Int64Counter {
    const metric = metricMeter.createCounter(this.name(name), undefined, help);
    return counter(metric, mergeLabels(this.#labels, labels));
  }

  public counterVec(name: string, help: string): Int64CounterVec {
    const metric = metricMeter.createCounter(this.name(name), undefined, help);
    return { with: (labels) => counter(metric, mergeLabels(this.#labels, labels)) };
  }

  public gauge(name: string, help: string, labels: Labels = {}): Int64Gauge {
    const metric = metricMeter.createGauge(this.name(name), "int", undefined, help);
    return gauge(metric, mergeLabels(this.#labels, labels));
  }

  public gaugeVec(name: string, help: string): Int64GaugeVec {
    const metric = metricMeter.createGauge(this.name(name), "int", undefined, help);
    return {
      with: (labels) => gauge(metric, mergeLabels(this.#labels, labels)),
      delete: (labels) => {
        metric.set(0, mergeLabels(this.#labels, labels));
      }
    };
  }

  public histogram(
    name: string,
    help: string,
    labels: Labels = {},
    buckets?: readonly number[]
  ): Float64Histogram {
    void buckets;
    const metric = metricMeter.createHistogram(this.name(name), "float", undefined, help);
    return histogram(metric, mergeLabels(this.#labels, labels));
  }

  public histogramVec(
    name: string,
    help: string,
    buckets?: readonly number[]
  ): Float64HistogramVec {
    void buckets;
    const metric = metricMeter.createHistogram(this.name(name), "float", undefined, help);
    return { with: (labels) => histogram(metric, mergeLabels(this.#labels, labels)) };
  }

  public observableFloat64Gauge(name: string, help: string, observe: () => number): void {
    void name;
    void help;
    void observe;
    // Workflow metrics have no process-owned scrape callback. Mutation points
    // emit gauges directly through Temporal's replay-aware metric meter.
  }

  private name(name: string): string {
    return name === "" ? this.#prefix : `${this.#prefix}_${name}`;
  }
}

export class WorkflowTracing implements Tracing {
  public enabled(): boolean {
    return true;
  }

  public tracer(name: string): Tracer {
    return new WorkflowTracer(trace.getTracer(name));
  }
}

class WorkflowTracer implements Tracer {
  readonly #tracer: ReturnType<typeof trace.getTracer>;

  public constructor(tracer: ReturnType<typeof trace.getTracer>) {
    this.#tracer = tracer;
  }

  public start(
    context: MessageContext,
    spanName: string,
    attributes: readonly Attribute[] = []
  ): StartedSpan {
    const parent = context.openTelemetryContext() ?? activeContext.active();
    const span = this.#tracer.startSpan(
      spanName,
      { attributes: otelAttributes(attributes) },
      parent
    );
    const next = trace.setSpan(parent, span);
    return {
      context: context.withOpenTelemetryContext(next),
      span: new WorkflowSpan(span)
    };
  }
}

class WorkflowSpan implements Span {
  readonly #span: OtelSpan;

  public constructor(span: OtelSpan) {
    this.#span = span;
  }

  public end(): void {
    this.#span.end();
  }
  public setAttributes(values: readonly Attribute[]): void {
    this.#span.setAttributes(otelAttributes(values));
  }
  public recordError(error: Error): void {
    this.#span.recordException(error);
  }
  public setStatus(code: SpanStatusCode, description: string): void {
    if (code === SpanStatusCode.Error) {
      this.#span.setStatus({ code: OtelSpanStatusCode.ERROR, message: description });
    } else {
      this.#span.setStatus({
        code: code === SpanStatusCode.Ok ? OtelSpanStatusCode.OK : OtelSpanStatusCode.UNSET
      });
    }
  }
  public addEvent(name: string, values: readonly Attribute[] = []): void {
    this.#span.addEvent(name, otelAttributes(values));
  }
  public spanContext(): SpanContext {
    const value = this.#span.spanContext();
    return {
      traceId: value.traceId,
      spanId: value.spanId,
      isValid: trace.isSpanContextValid(value)
    };
  }
}

function logFields(fields: readonly LogField[]): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const field of fields) {
    result[field.key] = field.type === "error" ? field.value.message : field.value;
  }
  return result;
}

function mergeLabels(base: Labels, extra: Labels): Record<string, string> {
  return { ...base, ...extra };
}

function counter(
  metric: ReturnType<typeof metricMeter.createCounter>,
  labels: Record<string, string>
): Int64Counter {
  return {
    inc: (context: Context) => {
      void context;
      metric.add(1, labels);
    },
    add: (context: Context, value: number) => {
      void context;
      metric.add(value, labels);
    }
  };
}

function gauge(
  metric: ReturnType<typeof metricMeter.createGauge>,
  labels: Record<string, string>
): Int64Gauge {
  let value = 0;
  const set = (next: number): void => {
    value = next;
    metric.set(next, labels);
  };
  return {
    set,
    inc: () => {
      set(value + 1);
    },
    dec: () => {
      set(value - 1);
    },
    add: (delta) => {
      set(value + delta);
    },
    sub: (delta) => {
      set(value - delta);
    }
  };
}

function histogram(
  metric: ReturnType<typeof metricMeter.createHistogram>,
  labels: Record<string, string>
): Float64Histogram {
  return {
    observe: (context: Context, value: number) => {
      void context;
      metric.record(value, labels);
    }
  };
}

function otelAttributes(values: readonly Attribute[]): Attributes {
  const result: Attributes = {};
  for (const value of values) {
    result[value.key] = value.type === "int64" ? Number(value.value) : value.value;
  }
  return result;
}
