import { trace } from "@opentelemetry/api";

import type { Context, MessageContext } from "../context.js";
import {
  LogLevel,
  type LogConfig,
  type LogField,
  type Logger,
  type LogsEngine
} from "../environment/log.js";

export interface JsonLogRecord {
  readonly timestamp: string;
  readonly severity: LogLevel;
  readonly message: string;
  readonly trace_id?: string;
  readonly span_id?: string;
  readonly fields?: Readonly<Record<string, unknown>>;
}

export interface JsonLogSink {
  write(record: JsonLogRecord): void;
}

export interface JsonLogsEngineOptions {
  readonly sink?: JsonLogSink;
  readonly now?: () => Date;
}

class StdoutJsonLogSink implements JsonLogSink {
  public write(record: JsonLogRecord): void {
    process.stdout.write(
      `${JSON.stringify(record, (_key, value: unknown) => jsonSafeValue(value))}\n`
    );
  }
}

export class JsonLogger implements Logger {
  readonly #sink: JsonLogSink;
  readonly #now: () => Date;

  public constructor(sink: JsonLogSink, now: () => Date = () => new Date()) {
    this.#sink = sink;
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
    const values: Record<string, unknown> = {};
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

export class JsonLogsEngine implements LogsEngine {
  readonly #logger: JsonLogger;

  public constructor(options: JsonLogsEngineOptions = {}) {
    this.#logger = new JsonLogger(options.sink ?? new StdoutJsonLogSink(), options.now);
  }

  public defaultLogger(config?: LogConfig): Logger {
    void config;
    return this.#logger;
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }
}

function jsonFieldValue(field: LogField): unknown {
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

function jsonSafeValue(value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Error) {
    return value.message;
  }
  return value;
}

function traceContext(
  context: Context
): { readonly traceId: string; readonly spanId: string } | undefined {
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

function isMessageContext(context: Context): context is MessageContext {
  return "openTelemetryContext" in context;
}
