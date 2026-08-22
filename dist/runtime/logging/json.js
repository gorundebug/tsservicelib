import { trace } from "@opentelemetry/api";
import { LogLevel } from "../environment/log.js";
class StdoutJsonLogSink {
    write(record) {
        process.stdout.write(`${JSON.stringify(record, (_key, value) => jsonSafeValue(value))}\n`);
    }
}
export class JsonLogger {
    #sink;
    #now;
    constructor(sink, now = () => new Date()) {
        this.#sink = sink;
        this.#now = now;
    }
    debug(context, message, ...fields) {
        this.emit(LogLevel.Debug, context, message, fields);
    }
    info(context, message, ...fields) {
        this.emit(LogLevel.Info, context, message, fields);
    }
    warn(context, message, ...fields) {
        this.emit(LogLevel.Warn, context, message, fields);
    }
    error(context, message, ...fields) {
        this.emit(LogLevel.Error, context, message, fields);
    }
    emit(severity, context, message, fields) {
        const values = {};
        for (const field of fields) {
            values[field.key] = jsonFieldValue(field);
        }
        const spanContext = traceContext(context);
        this.#sink.write({
            timestamp: this.#now().toISOString(),
            severity,
            message,
            ...(spanContext === undefined
                ? {}
                : { trace_id: spanContext.traceId, span_id: spanContext.spanId }),
            ...(fields.length === 0 ? {} : { fields: values })
        });
    }
}
export class JsonLogsEngine {
    #logger;
    constructor(options = {}) {
        this.#logger = new JsonLogger(options.sink ?? new StdoutJsonLogSink(), options.now);
    }
    defaultLogger(config) {
        void config;
        return this.#logger;
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
}
function jsonFieldValue(field) {
    switch (field.type) {
        case "int64":
            return field.value.toString();
        case "error":
            return field.value.message;
        case "any":
            return jsonSafeValue(field.value);
        default:
            return field.value;
    }
}
function jsonSafeValue(value) {
    if (typeof value === "bigint") {
        return value.toString();
    }
    if (value instanceof Error) {
        return value.message;
    }
    return value;
}
function traceContext(context) {
    const messageContext = isMessageContext(context) ? context : undefined;
    const openTelemetryContext = messageContext?.openTelemetryContext();
    if (openTelemetryContext === undefined) {
        return undefined;
    }
    const spanContext = trace.getSpanContext(openTelemetryContext);
    return spanContext === undefined
        ? undefined
        : { traceId: spanContext.traceId, spanId: spanContext.spanId };
}
function isMessageContext(context) {
    return "openTelemetryContext" in context;
}
//# sourceMappingURL=json.js.map