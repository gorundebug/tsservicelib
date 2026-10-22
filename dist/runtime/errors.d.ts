export declare class RuntimeStoppedError extends Error {
    constructor(message?: string);
}
export declare class RuntimeDrainTimeoutError extends Error {
    readonly timeoutMs: number;
    constructor(timeoutMs: number);
}
export declare function errorFromUnknown(value: unknown): Error;
/** Preserve ordinary cancellation without hiding unrelated failures after abort. */
export declare function isTaskCancellation(value: unknown, signal: AbortSignal): boolean;
/** Deliver an unhandled background failure to the host's uncaught-error policy. */
export declare function reportUnhandledTaskError(value: unknown): void;
//# sourceMappingURL=errors.d.ts.map