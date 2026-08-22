import type { Context } from "../context.js";

export const LogLevel = {
  Debug: "debug",
  Info: "info",
  Warn: "warn",
  Error: "error"
} as const;

export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];

export type LogField =
  | { readonly key: string; readonly type: "string"; readonly value: string }
  | { readonly key: string; readonly type: "int"; readonly value: number }
  | { readonly key: string; readonly type: "int64"; readonly value: bigint }
  | { readonly key: string; readonly type: "float64"; readonly value: number }
  | { readonly key: string; readonly type: "bool"; readonly value: boolean }
  | { readonly key: "error"; readonly type: "error"; readonly value: Error }
  | { readonly key: string; readonly type: "any"; readonly value: unknown };

export interface Logger {
  debug(context: Context, message: string, ...fields: readonly LogField[]): void;
  info(context: Context, message: string, ...fields: readonly LogField[]): void;
  warn(context: Context, message: string, ...fields: readonly LogField[]): void;
  error(context: Context, message: string, ...fields: readonly LogField[]): void;
}

export type LogConfig = Readonly<Record<string, never>>;

export interface LogsEngine {
  defaultLogger(config?: LogConfig): Logger;
  shutdown(context: Context): Promise<void>;
}

export class NoopLogger implements Logger {
  public debug(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    void message;
    void fields;
  }

  public info(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    void message;
    void fields;
  }

  public warn(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    void message;
    void fields;
  }

  public error(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    void message;
    void fields;
  }
}

export const noopLogger: Logger = new NoopLogger();

export class NoopLogsEngine implements LogsEngine {
  public defaultLogger(config?: LogConfig): Logger {
    void config;
    return noopLogger;
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }
}

export function str(key: string, value: string): LogField {
  return { key, type: "string", value };
}

export function int(key: string, value: number): LogField {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`log integer field ${key} must be a safe integer`);
  }
  return { key, type: "int", value };
}

export function int64(key: string, value: bigint): LogField {
  return { key, type: "int64", value };
}

export function float64(key: string, value: number): LogField {
  return { key, type: "float64", value };
}

export function bool(key: string, value: boolean): LogField {
  return { key, type: "bool", value };
}

export function err(value: Error): LogField {
  return { key: "error", type: "error", value };
}

export function any(key: string, value: unknown): LogField {
  return { key, type: "any", value };
}
