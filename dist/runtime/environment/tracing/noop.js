class NoopSpan {
    end() {
        return;
    }
    setAttributes(attributes) {
        void attributes;
    }
    recordError(error) {
        void error;
    }
    setStatus(code, description) {
        void code;
        void description;
    }
    addEvent(name, attributes) {
        void name;
        void attributes;
    }
    spanContext() {
        return invalidSpanContext;
    }
}
class NoopTracer {
    start(context, spanName) {
        void spanName;
        return { context, span: noopSpan };
    }
}
class NoopTracing {
    enabled() {
        return false;
    }
    tracer(name) {
        void name;
        return noopTracer;
    }
}
export class NoopTracingEngine {
    tracing() {
        return noopTracing;
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
}
const invalidSpanContext = { traceId: "", spanId: "", isValid: false };
export const noopSpan = new NoopSpan();
export const noopTracer = new NoopTracer();
export const noopTracing = new NoopTracing();
export const noopTracingEngine = new NoopTracingEngine();
//# sourceMappingURL=noop.js.map