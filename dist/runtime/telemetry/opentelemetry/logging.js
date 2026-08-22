import { ROOT_CONTEXT } from "@opentelemetry/api";
import { SeverityNumber } from "@opentelemetry/api-logs";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-grpc";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor, LoggerProvider } from "@opentelemetry/sdk-logs";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { LogLevel } from "../../environment/index.js";
export class OpenTelemetryLogsEngine {
    #provider;
    #logger;
    #shutdown;
    constructor(options) {
        const exporter = options.exporter ?? makeExporter(options);
        this.#provider = new LoggerProvider({
            resource: resourceFromAttributes({
                ...options.resourceAttributes,
                [ATTR_SERVICE_NAME]: options.serviceName
            }),
            processors: [new BatchLogRecordProcessor({ exporter, ...options.batch })]
        });
        this.#logger = new LoggerAdapter(this.#provider.getLogger(options.serviceName), options.now ?? (() => new Date()));
    }
    defaultLogger(config) {
        void config;
        return this.#logger;
    }
    async shutdown(context) {
        this.#shutdown ??= this.#provider.shutdown();
        await waitForShutdown(this.#shutdown, context.signal());
    }
}
class LoggerAdapter {
    #logger;
    #now;
    constructor(logger, now) {
        this.#logger = logger;
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
        const openTelemetryContext = contextForLog(context);
        const severityNumber = severityToOpenTelemetry(severity);
        if (!this.#logger.enabled({ context: openTelemetryContext, severityNumber }))
            return;
        const attributes = {};
        for (const field of fields)
            attributes[field.key] = fieldValue(field);
        this.#logger.emit({
            timestamp: this.#now(),
            severityNumber,
            severityText: severity,
            body: message,
            attributes,
            context: openTelemetryContext
        });
    }
}
function makeExporter(options) {
    const exporterOptions = {};
    if (options.endpoint !== undefined)
        exporterOptions.url = options.endpoint;
    if (options.exportTimeoutMillis !== undefined) {
        exporterOptions.timeoutMillis = options.exportTimeoutMillis;
    }
    return new OTLPLogExporter(exporterOptions);
}
function severityToOpenTelemetry(level) {
    switch (level) {
        case LogLevel.Debug:
            return SeverityNumber.DEBUG;
        case LogLevel.Info:
            return SeverityNumber.INFO;
        case LogLevel.Warn:
            return SeverityNumber.WARN;
        case LogLevel.Error:
            return SeverityNumber.ERROR;
    }
}
function fieldValue(field) {
    switch (field.type) {
        case "int64": {
            const value = Number(field.value);
            return Number.isSafeInteger(value) ? value : field.value.toString();
        }
        case "error":
            return field.value.message;
        case "any":
            return arbitraryString(field.value);
        default:
            return field.value;
    }
}
function arbitraryString(value) {
    if (value === undefined || value === null)
        return "";
    if (value instanceof Error)
        return value.message;
    switch (typeof value) {
        case "string":
            return value;
        case "number":
        case "boolean":
        case "bigint":
        case "symbol":
            return value.toString();
        case "function":
            return value.name;
        case "object": {
            const stringer = Reflect.get(value, "toString");
            return typeof stringer === "function" && stringer !== Object.prototype.toString
                ? stringer.call(value)
                : Object.prototype.toString.call(value);
        }
    }
    return "";
}
function contextForLog(context) {
    return isMessageContext(context)
        ? (context.openTelemetryContext() ?? ROOT_CONTEXT)
        : ROOT_CONTEXT;
}
function isMessageContext(context) {
    return "openTelemetryContext" in context;
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
    return signal.reason instanceof Error ? signal.reason : new Error("logging shutdown cancelled");
}
//# sourceMappingURL=logging.js.map