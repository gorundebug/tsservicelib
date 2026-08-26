import { context as activeContext, SpanStatusCode as OtelSpanStatusCode, trace } from "@opentelemetry/api";
import { log, metricMeter } from "@temporalio/workflow";
import { SpanStatusCode } from "../../runtime/environment/tracing/tracing.js";
export const workflowLogger = {
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
export class WorkflowMetrics {
    enabled() {
        return true;
    }
    scope(prefix, labels = {}) {
        return new WorkflowMetricsScope(prefix, labels);
    }
}
class WorkflowMetricsScope {
    #prefix;
    #labels;
    constructor(prefix, labels) {
        this.#prefix = prefix;
        this.#labels = labels;
    }
    counter(name, help, labels = {}) {
        const metric = metricMeter.createCounter(this.name(name), undefined, help);
        return counter(metric, mergeLabels(this.#labels, labels));
    }
    counterVec(name, help) {
        const metric = metricMeter.createCounter(this.name(name), undefined, help);
        return { with: (labels) => counter(metric, mergeLabels(this.#labels, labels)) };
    }
    gauge(name, help, labels = {}) {
        const metric = metricMeter.createGauge(this.name(name), "int", undefined, help);
        return gauge(metric, mergeLabels(this.#labels, labels));
    }
    gaugeVec(name, help) {
        const metric = metricMeter.createGauge(this.name(name), "int", undefined, help);
        return {
            with: (labels) => gauge(metric, mergeLabels(this.#labels, labels)),
            delete: (labels) => {
                metric.set(0, mergeLabels(this.#labels, labels));
            }
        };
    }
    histogram(name, help, labels = {}, buckets) {
        void buckets;
        const metric = metricMeter.createHistogram(this.name(name), "float", undefined, help);
        return histogram(metric, mergeLabels(this.#labels, labels));
    }
    histogramVec(name, help, buckets) {
        void buckets;
        const metric = metricMeter.createHistogram(this.name(name), "float", undefined, help);
        return { with: (labels) => histogram(metric, mergeLabels(this.#labels, labels)) };
    }
    observableFloat64Gauge(name, help, observe) {
        void name;
        void help;
        void observe;
        // Workflow metrics have no process-owned scrape callback. Mutation points
        // emit gauges directly through Temporal's replay-aware metric meter.
    }
    name(name) {
        return name === "" ? this.#prefix : `${this.#prefix}_${name}`;
    }
}
export class WorkflowTracing {
    enabled() {
        return true;
    }
    tracer(name) {
        return new WorkflowTracer(trace.getTracer(name));
    }
}
class WorkflowTracer {
    #tracer;
    constructor(tracer) {
        this.#tracer = tracer;
    }
    start(context, spanName, attributes = []) {
        const parent = context.openTelemetryContext() ?? activeContext.active();
        const span = this.#tracer.startSpan(spanName, { attributes: otelAttributes(attributes) }, parent);
        const next = trace.setSpan(parent, span);
        return {
            context: context.withOpenTelemetryContext(next),
            span: new WorkflowSpan(span)
        };
    }
}
class WorkflowSpan {
    #span;
    constructor(span) {
        this.#span = span;
    }
    end() {
        this.#span.end();
    }
    setAttributes(values) {
        this.#span.setAttributes(otelAttributes(values));
    }
    recordError(error) {
        this.#span.recordException(error);
    }
    setStatus(code, description) {
        if (code === SpanStatusCode.Error) {
            this.#span.setStatus({ code: OtelSpanStatusCode.ERROR, message: description });
        }
        else {
            this.#span.setStatus({
                code: code === SpanStatusCode.Ok ? OtelSpanStatusCode.OK : OtelSpanStatusCode.UNSET
            });
        }
    }
    addEvent(name, values = []) {
        this.#span.addEvent(name, otelAttributes(values));
    }
    spanContext() {
        const value = this.#span.spanContext();
        return {
            traceId: value.traceId,
            spanId: value.spanId,
            isValid: trace.isSpanContextValid(value)
        };
    }
}
function logFields(fields) {
    const result = {};
    for (const field of fields) {
        result[field.key] = field.type === "error" ? field.value.message : field.value;
    }
    return result;
}
function mergeLabels(base, extra) {
    return { ...base, ...extra };
}
function counter(metric, labels) {
    return {
        inc: (context) => {
            void context;
            metric.add(1, labels);
        },
        add: (context, value) => {
            void context;
            metric.add(value, labels);
        }
    };
}
function gauge(metric, labels) {
    let value = 0;
    const set = (next) => {
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
function histogram(metric, labels) {
    return {
        observe: (context, value) => {
            void context;
            metric.record(value, labels);
        }
    };
}
function otelAttributes(values) {
    const result = {};
    for (const value of values) {
        result[value.key] = value.type === "int64" ? Number(value.value) : value.value;
    }
    return result;
}
//# sourceMappingURL=workflow-telemetry.js.map