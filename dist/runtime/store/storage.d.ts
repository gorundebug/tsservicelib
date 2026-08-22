import type { Context } from "../context.js";
export interface Storage {
    start(context: Context): void | Promise<void>;
    stop(context: Context): void | Promise<void>;
}
export declare class StoreAlreadyStartedError extends Error {
    constructor();
}
export declare class StoreNotStartedError extends Error {
    constructor();
}
export declare class StoreStoppedError extends Error {
    constructor();
}
export declare class DuplicateKeyError extends Error {
    constructor(key: unknown);
}
//# sourceMappingURL=storage.d.ts.map