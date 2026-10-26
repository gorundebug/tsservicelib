export class ContextDeadlineExceededError extends Error {
    constructor() {
        super("context deadline exceeded");
        this.name = "ContextDeadlineExceededError";
    }
}
export class RuntimeStoppedError extends Error {
    constructor(message = "runtime is not accepting new work") {
        super(message);
        this.name = "RuntimeStoppedError";
    }
}
export class RuntimeDrainTimeoutError extends Error {
    timeoutMs;
    constructor(timeoutMs) {
        super(`runtime did not drain accepted work within ${String(timeoutMs)}ms`);
        this.timeoutMs = timeoutMs;
        this.name = "RuntimeDrainTimeoutError";
    }
}
export function errorFromUnknown(value) {
    if (value instanceof Error) {
        return value;
    }
    return new Error(String(value));
}
/** Preserve ordinary cancellation without hiding unrelated failures after abort. */
export function isTaskCancellation(value, signal) {
    return (signal.aborted &&
        (value === signal.reason || (value instanceof Error && value.name === "AbortError")));
}
/** Deliver an unhandled background failure to the host's uncaught-error policy. */
export function reportUnhandledTaskError(value) {
    const error = errorFromUnknown(value);
    queueMicrotask(() => {
        throw error;
    });
}
//# sourceMappingURL=errors.js.map