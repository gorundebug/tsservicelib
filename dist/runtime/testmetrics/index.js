export class TestMetrics {
    #families = new Map();
    #numbers = new Map();
    #histograms = new Map();
    #observables = new Map();
    enabled() {
        return true;
    }
    scope(prefix, labels = {}) {
        return new TestMetricsScope(this, prefix, labels);
    }
    registeredNames() {
        return [...this.#families.keys()].sort();
    }
    counterValue(name, labels = {}) {
        return this.#numbers.get(seriesKey(name, labels))?.value;
    }
    gaugeValue(name, labels = {}) {
        return this.#numbers.get(seriesKey(name, labels))?.value;
    }
    histogramValue(name, labels = {}) {
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
    observableGaugeValue(name, labels = {}) {
        return this.#observables.get(seriesKey(name, labels))?.observe();
    }
    deleteNumberInstrument(name, labels) {
        this.#numbers.delete(seriesKey(name, labels));
    }
    numberInstrument(kind, name, help, labels) {
        this.registerFamily(name, kind, help);
        const key = seriesKey(name, labels);
        let series = this.#numbers.get(key);
        if (series === undefined) {
            series = { value: 0 };
            this.#numbers.set(key, series);
        }
        return new TestNumberInstrument(kind, series);
    }
    histogramInstrument(name, help, labels) {
        this.registerFamily(name, "histogram", help);
        const key = seriesKey(name, labels);
        let series = this.#histograms.get(key);
        if (series === undefined) {
            series = { values: [] };
            this.#histograms.set(key, series);
        }
        return new TestHistogram(series);
    }
    observableInstrument(name, help, labels, observe) {
        this.registerFamily(name, "observable-gauge", help);
        const key = seriesKey(name, labels);
        if (this.#observables.has(key)) {
            throw new Error(`metric series ${key} is already registered`);
        }
        this.#observables.set(key, { observe });
    }
    registerFamily(name, kind, help) {
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
class TestMetricsScope {
    #metrics;
    #prefix;
    #labels;
    constructor(metrics, prefix, labels) {
        this.#metrics = metrics;
        this.#prefix = prefix;
        this.#labels = { ...labels };
    }
    counter(name, help, labels = {}) {
        return this.#metrics.numberInstrument("counter", metricName(this.#prefix, name), help, mergeLabels(this.#labels, labels));
    }
    counterVec(name, help) {
        return new TestCounterVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
    }
    gauge(name, help, labels = {}) {
        return this.#metrics.numberInstrument("gauge", metricName(this.#prefix, name), help, mergeLabels(this.#labels, labels));
    }
    gaugeVec(name, help) {
        return new TestGaugeVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
    }
    histogram(name, help, labels = {}, buckets) {
        void buckets;
        return this.#metrics.histogramInstrument(metricName(this.#prefix, name), help, mergeLabels(this.#labels, labels));
    }
    histogramVec(name, help, buckets) {
        void buckets;
        return new TestHistogramVec(this.#metrics, metricName(this.#prefix, name), help, this.#labels);
    }
    observableFloat64Gauge(name, help, observe) {
        this.#metrics.observableInstrument(metricName(this.#prefix, name), help, this.#labels, observe);
    }
}
class TestNumberInstrument {
    #kind;
    #series;
    constructor(kind, series) {
        this.#kind = kind;
        this.#series = series;
    }
    inc(context) {
        void context;
        this.addValue(1);
    }
    dec() {
        this.addValue(-1);
    }
    add(contextOrDelta, value) {
        const delta = typeof contextOrDelta === "number" ? contextOrDelta : value;
        if (delta === undefined) {
            throw new Error("counter add value is missing");
        }
        this.addValue(delta);
    }
    sub(delta) {
        this.addValue(-delta);
    }
    set(value) {
        if (this.#kind !== "gauge") {
            throw new Error("counter cannot be set");
        }
        requireFiniteSafeInteger(value);
        this.#series.value = value;
    }
    addValue(delta) {
        requireFiniteSafeInteger(delta);
        if (this.#kind === "counter" && delta < 0) {
            throw new RangeError("counter cannot decrease");
        }
        this.#series.value += delta;
    }
}
class TestHistogram {
    #series;
    constructor(series) {
        this.#series = series;
    }
    observe(context, value) {
        void context;
        if (!Number.isFinite(value)) {
            throw new RangeError("histogram observation must be finite");
        }
        this.#series.values.push(value);
    }
}
class TestCounterVec {
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
        return this.#metrics.numberInstrument("counter", this.#name, this.#help, mergeLabels(this.#labels, labels));
    }
}
class TestGaugeVec {
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
        return this.#metrics.numberInstrument("gauge", this.#name, this.#help, mergeLabels(this.#labels, labels));
    }
    delete(labels) {
        this.#metrics.deleteNumberInstrument(this.#name, mergeLabels(this.#labels, labels));
    }
}
class TestHistogramVec {
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
        return this.#metrics.histogramInstrument(this.#name, this.#help, mergeLabels(this.#labels, labels));
    }
}
function metricName(prefix, name) {
    return prefix.length === 0 ? name : `${prefix}_${name}`;
}
function mergeLabels(base, extra) {
    return { ...base, ...extra };
}
function seriesKey(name, labels) {
    const suffix = Object.entries(labels)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
        .join(",");
    return `${name}{${suffix}}`;
}
function requireFiniteSafeInteger(value) {
    if (!Number.isSafeInteger(value)) {
        throw new RangeError("integer metric value must be a safe integer");
    }
}
//# sourceMappingURL=index.js.map