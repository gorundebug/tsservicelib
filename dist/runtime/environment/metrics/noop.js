class NoopInstrument {
    inc() {
        return;
    }
    dec() {
        return;
    }
    add() {
        return;
    }
    sub() {
        return;
    }
    set() {
        return;
    }
    observe() {
        return;
    }
}
class NoopVector {
    with(labels) {
        void labels;
        return noopInstrument;
    }
    delete(labels) {
        void labels;
    }
}
class NoopMetricsScope {
    counter(name, help, labels) {
        void name;
        void help;
        void labels;
        return noopInstrument;
    }
    counterVec(name, help) {
        void name;
        void help;
        return noopVector;
    }
    gauge(name, help, labels) {
        void name;
        void help;
        void labels;
        return noopInstrument;
    }
    gaugeVec(name, help) {
        void name;
        void help;
        return noopVector;
    }
    histogram(name, help, labels, buckets) {
        void name;
        void help;
        void labels;
        void buckets;
        return noopInstrument;
    }
    histogramVec(name, help, buckets) {
        void name;
        void help;
        void buckets;
        return noopVector;
    }
    observableFloat64Gauge(name, help, observe) {
        void name;
        void help;
        void observe;
    }
}
class NoopMetrics {
    enabled() {
        return false;
    }
    scope(prefix, labels) {
        void prefix;
        void labels;
        return noopScope;
    }
}
export class NoopMetricsEngine {
    metrics() {
        return noopMetrics;
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
    contentType() {
        return "text/plain; version=0.0.4; charset=utf-8";
    }
    render() {
        return Promise.resolve("# ServiceLib runtime metrics are disabled.\n");
    }
}
const noopInstrument = new NoopInstrument();
const noopVector = new NoopVector();
const noopScope = new NoopMetricsScope();
export const noopMetrics = new NoopMetrics();
export const noopMetricsEngine = new NoopMetricsEngine();
//# sourceMappingURL=noop.js.map