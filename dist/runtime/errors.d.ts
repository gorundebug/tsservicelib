export declare class RuntimeStoppedError extends Error {
    constructor(message?: string);
}
export declare class RuntimeDrainTimeoutError extends Error {
    readonly timeoutMs: number;
    constructor(timeoutMs: number);
}
export declare function errorFromUnknown(value: unknown): Error;
//# sourceMappingURL=errors.d.ts.map