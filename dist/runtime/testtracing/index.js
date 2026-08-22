import { SpanStatusCode as Status } from "../environment/index.js";
export class TestTracing {
    #spans = [];
    enabled() {
        return true;
    }
    tracing() {
        return this;
    }
    tracer(name) {
        return new TestTracer(this, name);
    }
    spans() {
        return this.#spans.map((span) => ({
            ...span,
            attributes: [...span.attributes],
            events: span.events.map((event) => ({ ...event, attributes: [...event.attributes] }))
        }));
    }
    reset() {
        this.#spans.length = 0;
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
    record(span) {
        this.#spans.push(span);
    }
}
class TestTracer {
    #engine;
    #name;
    constructor(engine, name) {
        this.#engine = engine;
        this.#name = name;
    }
    start(context, spanName, attributes = []) {
        return {
            context,
            span: new TestSpan(this.#engine, this.#name, spanName, attributes)
        };
    }
}
class TestSpan {
    #engine;
    #tracerName;
    #name;
    #attributes;
    #events = [];
    #statusCode = Status.Unset;
    #statusDescription = "";
    #error;
    #ended = false;
    constructor(engine, tracerName, name, attributes) {
        this.#engine = engine;
        this.#tracerName = tracerName;
        this.#name = name;
        this.#attributes = [...attributes];
    }
    end() {
        if (this.#ended) {
            return;
        }
        this.#ended = true;
        this.#engine.record({
            tracerName: this.#tracerName,
            name: this.#name,
            attributes: [...this.#attributes],
            events: [...this.#events],
            statusCode: this.#statusCode,
            statusDescription: this.#statusDescription,
            error: this.#error
        });
    }
    setAttributes(attributes) {
        this.#attributes.push(...attributes);
    }
    recordError(error) {
        this.#error = error;
    }
    setStatus(code, description) {
        this.#statusCode = code;
        this.#statusDescription = description;
    }
    addEvent(name, attributes = []) {
        this.#events.push({ name, attributes: [...attributes] });
    }
    spanContext() {
        return { traceId: "", spanId: "", isValid: false };
    }
}
//# sourceMappingURL=index.js.map