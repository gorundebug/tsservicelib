import { ROOT_CONTEXT, SpanStatusCode as OpenTelemetrySpanStatusCode, trace } from "@opentelemetry/api";
import { CompositePropagator, W3CBaggagePropagator, W3CTraceContextPropagator } from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { AlwaysOnSampler, BatchSpanProcessor, NodeTracerProvider, ParentBasedSampler } from "@opentelemetry/sdk-trace-node";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { SpanStatusCode } from "../../environment/index.js";
const propagator = new CompositePropagator({
    propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()]
});
const metadataGetter = {
    keys(carrier) {
        return [...carrier.keys()];
    },
    get(carrier, key) {
        return carrier.get(key.toLowerCase());
    }
};
const metadataSetter = {
    set(carrier, key, value) {
        carrier.set(key.toLowerCase(), value);
    }
};
export class OpenTelemetryTracingEngine {
    #provider;
    #tracing;
    #shutdown;
    constructor(options) {
        const exporter = options.exporter ?? makeExporter(options);
        const processor = new BatchSpanProcessor(exporter, options.batch);
        this.#provider = new NodeTracerProvider({
            resource: resourceFromAttributes({
                ...options.resourceAttributes,
                [ATTR_SERVICE_NAME]: options.serviceName
            }),
            sampler: new ParentBasedSampler({ root: new AlwaysOnSampler() }),
            spanProcessors: [processor]
        });
        this.#tracing = new OpenTelemetryTracing(this.#provider);
    }
    tracing() {
        return this.#tracing;
    }
    async shutdown(context) {
        this.#shutdown ??= this.#provider.shutdown();
        await waitForShutdown(this.#shutdown, context.signal());
    }
}
class OpenTelemetryTracing {
    #provider;
    constructor(provider) {
        this.#provider = provider;
    }
    enabled() {
        return true;
    }
    tracer(name) {
        return new TracerAdapter(this.#provider.getTracer(name));
    }
}
class TracerAdapter {
    #tracer;
    constructor(tracer) {
        this.#tracer = tracer;
    }
    start(context, spanName, attributes = []) {
        const parent = parentContext(context);
        const span = this.#tracer.startSpan(spanName, { attributes: attributesToOpenTelemetry(attributes) }, parent);
        const active = trace.setSpan(parent, span);
        const metadata = new Map(context.metadata());
        propagator.inject(active, metadata, metadataSetter);
        return {
            context: context.withMetadata(metadata).withOpenTelemetryContext(active),
            span: new SpanAdapter(span)
        };
    }
}
class SpanAdapter {
    #span;
    #ended = false;
    constructor(span) {
        this.#span = span;
    }
    end() {
        if (this.#ended) {
            return;
        }
        this.#ended = true;
        this.#span.end();
    }
    setAttributes(attributes) {
        this.#span.setAttributes(attributesToOpenTelemetry(attributes));
    }
    recordError(error) {
        this.#span.recordException(error);
    }
    setStatus(code, description) {
        const statusCode = statusCodeToOpenTelemetry(code);
        if (statusCode === OpenTelemetrySpanStatusCode.ERROR) {
            this.#span.setStatus({ code: statusCode, message: description });
            return;
        }
        this.#span.setStatus({ code: statusCode });
    }
    addEvent(name, attributes = []) {
        this.#span.addEvent(name, attributesToOpenTelemetry(attributes));
    }
    spanContext() {
        const context = this.#span.spanContext();
        return {
            traceId: context.traceId,
            spanId: context.spanId,
            isValid: trace.isSpanContextValid(context)
        };
    }
}
function makeExporter(options) {
    const exporterOptions = {};
    if (options.endpoint !== undefined) {
        exporterOptions.url = options.endpoint;
    }
    if (options.exportTimeoutMillis !== undefined) {
        exporterOptions.timeoutMillis = options.exportTimeoutMillis;
    }
    return new OTLPTraceExporter(exporterOptions);
}
function parentContext(context) {
    const current = context.openTelemetryContext();
    if (current !== undefined) {
        return current;
    }
    return propagator.extract(ROOT_CONTEXT, context.metadata(), metadataGetter);
}
function attributesToOpenTelemetry(attributes) {
    const result = {};
    for (const attribute of attributes) {
        result[attribute.key] = attributeValue(attribute);
    }
    return result;
}
function attributeValue(attribute) {
    if (attribute.type !== "int64") {
        return attribute.value;
    }
    const value = Number(attribute.value);
    if (!Number.isSafeInteger(value)) {
        throw new RangeError(`OpenTelemetry JS cannot represent int64 attribute ${attribute.key}=${attribute.value.toString()} exactly`);
    }
    return value;
}
function statusCodeToOpenTelemetry(code) {
    switch (code) {
        case SpanStatusCode.Ok:
            return OpenTelemetrySpanStatusCode.OK;
        case SpanStatusCode.Error:
            return OpenTelemetrySpanStatusCode.ERROR;
        case SpanStatusCode.Unset:
            return OpenTelemetrySpanStatusCode.UNSET;
    }
}
async function waitForShutdown(shutdown, signal) {
    if (signal.aborted) {
        void shutdown.catch(() => undefined);
        throw cancellationError(signal);
    }
    let rejectCancellation;
    const cancelled = new Promise((_resolve, reject) => {
        rejectCancellation = reject;
    });
    const onAbort = () => {
        rejectCancellation?.(cancellationError(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    try {
        await Promise.race([shutdown, cancelled]);
    }
    finally {
        signal.removeEventListener("abort", onAbort);
        void shutdown.catch(() => undefined);
    }
}
function cancellationError(signal) {
    return signal.reason instanceof Error ? signal.reason : new Error("tracing shutdown cancelled");
}
//# sourceMappingURL=tracing.js.map