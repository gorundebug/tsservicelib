import type { Context } from "../context.js";
import { type LogConfig, type LogField, type Logger, type LogLevel, type LogsEngine } from "../environment/log.js";
export interface LogEntry {
    readonly level: LogLevel;
    readonly message: string;
    readonly fields: readonly LogField[];
}
export declare class TestLog implements LogsEngine {
    #private;
    defaultLogger(config?: LogConfig): Logger;
    shutdown(context: Context): Promise<void>;
    entries(): readonly LogEntry[];
    entriesAtLevel(level: LogLevel): readonly LogEntry[];
    reset(): void;
}
//# sourceMappingURL=index.d.ts.map