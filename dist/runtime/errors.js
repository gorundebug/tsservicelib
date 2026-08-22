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
//# sourceMappingURL=errors.js.map