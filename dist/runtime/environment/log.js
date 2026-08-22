export const LogLevel = {
    Debug: "debug",
    Info: "info",
    Warn: "warn",
    Error: "error"
};
export class NoopLogger {
    debug(context, message, ...fields) {
        void context;
        void message;
        void fields;
    }
    info(context, message, ...fields) {
        void context;
        void message;
        void fields;
    }
    warn(context, message, ...fields) {
        void context;
        void message;
        void fields;
    }
    error(context, message, ...fields) {
        void context;
        void message;
        void fields;
    }
}
export const noopLogger = new NoopLogger();
export class NoopLogsEngine {
    defaultLogger(config) {
        void config;
        return noopLogger;
    }
    shutdown(context) {
        void context;
        return Promise.resolve();
    }
}
export function str(key, value) {
    return { key, type: "string", value };
}
export function int(key, value) {
    if (!Number.isSafeInteger(value)) {
        throw new RangeError(`log integer field ${key} must be a safe integer`);
    }
    return { key, type: "int", value };
}
export function int64(key, value) {
    return { key, type: "int64", value };
}
export function float64(key, value) {
    return { key, type: "float64", value };
}
export function bool(key, value) {
    return { key, type: "bool", value };
}
export function err(value) {
    return { key: "error", type: "error", value };
}
export function any(key, value) {
    return { key, type: "any", value };
}
//# sourceMappingURL=log.js.map