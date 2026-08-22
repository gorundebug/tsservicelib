import { ROOT_CONTEXT, type Context as OpenTelemetryContext } from "@opentelemetry/api";
import {
  SeverityNumber,
  type AnyValue,
  type Logger as OpenTelemetryLogger
} from "@opentelemetry/api-logs";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-grpc";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  BatchLogRecordProcessor,
  LoggerProvider,
  type BatchLogRecordProcessorOptions,
  type LogRecordExporter
} from "@opentelemetry/sdk-logs";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";

import type { Context, MessageContext } from "../../context.js";
import {
  LogLevel,
  type LogConfig,
  type LogField,
  type Logger,
  type LogsEngine
} from "../../environment/index.js";

export interface OpenTelemetryLogsOptions {
  readonly serviceName: string;
  readonly endpoint?: string;
  readonly exportTimeoutMillis?: number;
  readonly exporter?: LogRecordExporter;
  readonly batch?: Omit<BatchLogRecordProcessorOptions, "exporter">;
  readonly resourceAttributes?: Readonly<Record<string, string | number | boolean>>;
  readonly now?: () => Date;
}

export class OpenTelemetryLogsEngine implements LogsEngine {
  readonly #provider: LoggerProvider;
  readonly #logger: Logger;
  #shutdown: Promise<void> | undefined;

  public constructor(options: OpenTelemetryLogsOptions) {
    const exporter = options.exporter ?? makeExporter(options);
    this.#provider = new LoggerProvider({
      resource: resourceFromAttributes({
        ...options.resourceAttributes,
        [ATTR_SERVICE_NAME]: options.serviceName
      }),
      processors: [new BatchLogRecordProcessor({ exporter, ...options.batch })]
    });
    this.#logger = new LoggerAdapter(
      this.#provider.getLogger(options.serviceName),
      options.now ?? (() => new Date())
    );
  }

  public defaultLogger(config?: LogConfig): Logger {
    void config;
    return this.#logger;
  }

  public async shutdown(context: Context): Promise<void> {
    this.#shutdown ??= this.#provider.shutdown();
    await waitForShutdown(this.#shutdown, context.signal());
  }
}

class LoggerAdapter implements Logger {
  readonly #logger: OpenTelemetryLogger;
  readonly #now: () => Date;

  public constructor(logger: OpenTelemetryLogger, now: () => Date) {
    this.#logger = logger;
    this.#now = now;
  }

  public debug(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.emit(LogLevel.Debug, context, message, fields);
  }

  public info(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.emit(LogLevel.Info, context, message, fields);
  }

  public warn(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.emit(LogLevel.Warn, context, message, fields);
  }

  public error(context: Context, message: string, ...fields: readonly LogField[]): void {
    this.emit(LogLevel.Error, context, message, fields);
  }

  private emit(
    severity: LogLevel,
    context: Context,
    message: string,
    fields: readonly LogField[]
  ): void {
    const openTelemetryContext = contextForLog(context);
    const severityNumber = severityToOpenTelemetry(severity);
    if (!this.#logger.enabled({ context: openTelemetryContext, severityNumber })) return;
    const attributes: Record<string, AnyValue> = {};
    for (const field of fields) attributes[field.key] = fieldValue(field);
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

function makeExporter(options: OpenTelemetryLogsOptions): LogRecordExporter {
  const exporterOptions: { url?: string; timeoutMillis?: number } = {};
  if (options.endpoint !== undefined) exporterOptions.url = options.endpoint;
  if (options.exportTimeoutMillis !== undefined) {
    exporterOptions.timeoutMillis = options.exportTimeoutMillis;
  }
  return new OTLPLogExporter(exporterOptions);
}

function severityToOpenTelemetry(level: LogLevel): SeverityNumber {
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

function fieldValue(field: LogField): AnyValue {
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

function arbitraryString(value: unknown): string {
  if (value === undefined || value === null) return "";
  if (value instanceof Error) return value.message;
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
      const stringer: unknown = Reflect.get(value, "toString");
      return typeof stringer === "function" && stringer !== Object.prototype.toString
        ? (stringer as (this: object) => string).call(value)
        : Object.prototype.toString.call(value);
    }
  }
  return "";
}

function contextForLog(context: Context): OpenTelemetryContext {
  return isMessageContext(context)
    ? (context.openTelemetryContext() ?? ROOT_CONTEXT)
    : ROOT_CONTEXT;
}

function isMessageContext(context: Context): context is MessageContext {
  return "openTelemetryContext" in context;
}

async function waitForShutdown(shutdown: Promise<void>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    void shutdown.catch(() => undefined);
    throw cancellationError(signal);
  }
  let rejectCancellation: ((reason: Error) => void) | undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    rejectCancellation = reject;
  });
  const onAbort = (): void => {
    rejectCancellation?.(cancellationError(signal));
  };
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    await Promise.race([shutdown, cancelled]);
  } finally {
    signal.removeEventListener("abort", onAbort);
    void shutdown.catch(() => undefined);
  }
}

function cancellationError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error("logging shutdown cancelled");
}
