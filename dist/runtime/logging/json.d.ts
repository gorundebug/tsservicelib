import type { Context } from "../context.js";
import { LogLevel, type LogConfig, type LogField, type Logger, type LogsEngine } from "../environment/log.js";
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
export declare class JsonLogger implements Logger {
    #private;
    constructor(sink: JsonLogSink, now?: () => Date);
    debug(context: Context, message: string, ...fields: readonly LogField[]): void;
    info(context: Context, message: string, ...fields: readonly LogField[]): void;
    warn(context: Context, message: string, ...fields: readonly LogField[]): void;
    error(context: Context, message: string, ...fields: readonly LogField[]): void;
    private emit;
}
export declare class JsonLogsEngine implements LogsEngine {
    #private;
    constructor(options?: JsonLogsEngineOptions);
    defaultLogger(config?: LogConfig): Logger;
    shutdown(context: Context): Promise<void>;
}
//# sourceMappingURL=json.d.ts.map