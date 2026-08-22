import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";
export class PrometheusMetrics {
    #registry;
    #families = new Map();
    constructor(registry = new Registry()) {
        this.#registry = registry;
        // Use prom-client's standard Node.js/process collectors instead of
        // inventing framework-specific heap, process and V8 runtime metrics.
        // PrometheusMetrics is instantiated only when runtime telemetry is enabled;
        // stripped benchmark/profiling builds keep the zero-work Noop engine.
        collectDefaultMetrics({ register: this.#registry });
    }
    enabled() {
        return true;
    }
    scope(prefix, labels = {}) {
        return new PrometheusMetricsScope(this, prefix, labels);
    }
    registry() {
        return this.#registry;
    }
    counter(name, help, labels) {
        const labelNames = sortedLabelNames(labels);
        const existing = this.#families.get(name);
        let family;
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
        }
        else {
            requireFamily(existing, "counter", name, help, labelNames);
            family = existing;
        }
        return new PrometheusCounter(family.metric, labels);
    }
    gauge(name, help, labels) {
        const labelNames = sortedLabelNames(labels);
        const existing = this.#families.get(name);
        let family;
        if (existing === undefined) {
            family = {
                kind: "gauge",
                help,
                labelNames,
                metric: new Gauge({ name, help, labelNames: [...labelNames], registers: [this.#registry] })
            };
            this.#families.set(name, family);
        }
        else {
            requireFamily(existing, "gauge", name, help, labelNames);
            family = existing;
        }
        return new PrometheusGauge(family.metric, labels);
    }
    deleteGauge(name, help, labels) {
        const existing = this.#families.get(name);
        if (existing === undefined) {
            return;
        }
        requireFamily(existing, "gauge", name, help, sortedLabelNames(labels));
        existing.metric.remove(labels);
    }
    histogram(name, help, labels, buckets) {
        const labelNames = sortedLabelNames(labels);
        const existing = this.#families.get(name);
        let family;
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
        }
        else {
            requireFamily(existing, "histogram", name, help, labelNames);
            if (!sameNumbers(existing.buckets, buckets)) {
                throw new Error(`metric ${name} is already registered with different buckets`);
            }
            family = existing;
        }
        return new PrometheusHistogram(family.metric, labels);
    }
    observableGauge(name, help, labels, observe) {
        const labelNames = sortedLabelNames(labels);
        const existing = this.#families.get(name);
        let family;
        if (existing === undefined) {
            const observers = new Map();
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
        }
        else {
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
export class PrometheusMetricsEngine {
    #metrics;
    constructor(metrics = new PrometheusMetrics()) {
        this.#metrics = metrics;
    }
    metrics() {
        return this.#metrics;
    }
    contentType() {
        return this.#metrics.registry().contentType;
    }
    render() {
        return this.#metrics.registry().metrics();
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
}
class PrometheusMetricsScope {
    #metrics;
    #prefix;
    #labels;
    constructor(metrics, prefix, labels) {
        this.#metrics = metrics;
        this.#prefix = prefix;
        this.#labels = { ...labels };
    }
    counter(name, help, labels = {}) {
        return this.#metrics.counter(metricName(this.#prefix, name), help, mergeLabels(this.#labels, labels));
    }
    counterVec(name, help) {
        return new PrometheusCounterVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
    }
    gauge(name, help, labels = {}) {
        return this.#metrics.gauge(metricName(this.#prefix, name), help, mergeLabels(this.#labels, labels));
    }
    gaugeVec(name, help) {
        return new PrometheusGaugeVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
    }
    histogram(name, help, labels = {}, buckets) {
        return this.#metrics.histogram(metricName(this.#prefix, name), help, mergeLabels(this.#labels, labels), buckets);
    }
    histogramVec(name, help, buckets) {
        return new PrometheusHistogramVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels, buckets);
    }
    observableFloat64Gauge(name, help, observe) {
        this.#metrics.observableGauge(metricName(this.#prefix, name), help, this.#labels, observe);
    }
}
class PrometheusCounter {
    #metric;
    #labels;
    constructor(metric, labels) {
        this.#metric = metric;
        this.#labels = labels;
    }
    inc(context) {
        this.add(context, 1);
    }
    add(context, value) {
        void context;
        requireSafeInteger(value);
        if (value < 0) {
            throw new RangeError("counter cannot decrease");
        }
        this.#metric.inc(this.#labels, value);
    }
}
class PrometheusGauge {
    #metric;
    #labels;
    constructor(metric, labels) {
        this.#metric = metric;
        this.#labels = labels;
    }
    set(value) {
        requireSafeInteger(value);
        this.#metric.set(this.#labels, value);
    }
    inc() {
        this.#metric.inc(this.#labels);
    }
    dec() {
        this.#metric.dec(this.#labels);
    }
    add(delta) {
        requireSafeInteger(delta);
        this.#metric.inc(this.#labels, delta);
    }
    sub(delta) {
        requireSafeInteger(delta);
        this.#metric.dec(this.#labels, delta);
    }
}
class PrometheusHistogram {
    #metric;
    #labels;
    constructor(metric, labels) {
        this.#metric = metric;
        this.#labels = labels;
    }
    observe(context, value) {
        void context;
        if (!Number.isFinite(value)) {
            throw new RangeError("histogram observation must be finite");
        }
        this.#metric.observe(this.#labels, value);
    }
}
class PrometheusCounterVec {
    #metrics;
    #name;
    #help;
    #labels;
    constructor(metrics, name, help, labels) {
        this.#metrics = metrics;
        this.#name = name;
        this.#help = help;
        this.#labels = labels;
    }
    with(labels) {
        return this.#metrics.counter(this.#name, this.#help, mergeLabels(this.#labels, labels));
    }
}
class PrometheusGaugeVec {
    #metrics;
    #name;
    #help;
    #labels;
    constructor(metrics, name, help, labels) {
        this.#metrics = metrics;
        this.#name = name;
        this.#help = help;
        this.#labels = labels;
    }
    with(labels) {
        return this.#metrics.gauge(this.#name, this.#help, mergeLabels(this.#labels, labels));
    }
    delete(labels) {
        this.#metrics.deleteGauge(this.#name, this.#help, mergeLabels(this.#labels, labels));
    }
}
class PrometheusHistogramVec {
    #metrics;
    #name;
    #help;
    #labels;
    #buckets;
    constructor(metrics, name, help, labels, buckets) {
        this.#metrics = metrics;
        this.#name = name;
        this.#help = help;
        this.#labels = labels;
        this.#buckets = buckets;
    }
    with(labels) {
        return this.#metrics.histogram(this.#name, this.#help, mergeLabels(this.#labels, labels), this.#buckets);
    }
}
function requireFamily(family, kind, name, help, labelNames) {
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
function metricName(prefix, name) {
    return prefix.length === 0 ? name : `${prefix}_${name}`;
}
function mergeLabels(base, extra) {
    return { ...base, ...extra };
}
function sortedLabelNames(labels) {
    return Object.keys(labels).sort();
}
function labelsKey(labels) {
    return Object.entries(labels)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
        .join(",");
}
function sameStrings(left, right) {
    return left.length === right.length && left.every((value, index) => value === right[index]);
}
function sameNumbers(left, right) {
    if (left === undefined || right === undefined) {
        return left === right;
    }
    return left.length === right.length && left.every((value, index) => value === right[index]);
}
function requireSafeInteger(value) {
    if (!Number.isSafeInteger(value)) {
        throw new RangeError("integer metric value must be a safe integer");
    }
}
//# sourceMappingURL=prometheus.js.map