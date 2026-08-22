import type { Context } from "../context.js";
import {
  type LogConfig,
  type LogField,
  type Logger,
  type LogLevel,
  type LogsEngine
} from "../environment/log.js";

export interface LogEntry {
  readonly level: LogLevel;
  readonly message: string;
  readonly fields: readonly LogField[];
}

class TestLogger implements Logger {
  readonly #record: (entry: LogEntry) => void;

  public constructor(record: (entry: LogEntry) => void) {
    this.#record = record;
  }

  public debug(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    this.#record({ level: "debug", message, fields: [...fields] });
  }

  public info(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    this.#record({ level: "info", message, fields: [...fields] });
  }

  public warn(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    this.#record({ level: "warn", message, fields: [...fields] });
  }

  public error(context: Context, message: string, ...fields: readonly LogField[]): void {
    void context;
    this.#record({ level: "error", message, fields: [...fields] });
  }
}

export class TestLog implements LogsEngine {
  readonly #entries: LogEntry[] = [];
  readonly #logger = new TestLogger((entry) => this.#entries.push(entry));

  public defaultLogger(config?: LogConfig): Logger {
    void config;
    return this.#logger;
  }

  public shutdown(context: Context): Promise<void> {
    void context;
    return Promise.resolve();
  }

  public entries(): readonly LogEntry[] {
    return this.#entries.map((entry) => ({ ...entry, fields: [...entry.fields] }));
  }

  public entriesAtLevel(level: LogLevel): readonly LogEntry[] {
    return this.entries().filter((entry) => entry.level === level);
  }

  public reset(): void {
    this.#entries.length = 0;
  }
}
