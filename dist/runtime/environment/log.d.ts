import type { Context } from "../context.js";
export declare const LogLevel: {
    readonly Debug: "debug";
    readonly Info: "info";
    readonly Warn: "warn";
    readonly Error: "error";
};
export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];
export type LogField = {
    readonly key: string;
    readonly type: "string";
    readonly value: string;
} | {
    readonly key: string;
    readonly type: "int";
    readonly value: number;
} | {
    readonly key: string;
    readonly type: "int64";
    readonly value: bigint;
} | {
    readonly key: string;
    readonly type: "float64";
    readonly value: number;
} | {
    readonly key: string;
    readonly type: "bool";
    readonly value: boolean;
} | {
    readonly key: "error";
    readonly type: "error";
    readonly value: Error;
} | {
    readonly key: string;
    readonly type: "any";
    readonly value: unknown;
};
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
export declare class NoopLogger implements Logger {
    debug(context: Context, message: string, ...fields: readonly LogField[]): void;
    info(context: Context, message: string, ...fields: readonly LogField[]): void;
    warn(context: Context, message: string, ...fields: readonly LogField[]): void;
    error(context: Context, message: string, ...fields: readonly LogField[]): void;
}
export declare const noopLogger: Logger;
export declare class NoopLogsEngine implements LogsEngine {
    defaultLogger(config?: LogConfig): Logger;
    shutdown(context: Context): Promise<void>;
}
export declare function str(key: string, value: string): LogField;
export declare function int(key: string, value: number): LogField;
export declare function int64(key: string, value: bigint): LogField;
export declare function float64(key: string, value: number): LogField;
export declare function bool(key: string, value: boolean): LogField;
export declare function err(value: Error): LogField;
export declare function any(key: string, value: unknown): LogField;
//# sourceMappingURL=log.d.ts.map