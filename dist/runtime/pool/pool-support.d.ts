import type { PoolTask } from "./pool.js";
export declare function bindPoolTask(execute: PoolTask): PoolTask;
export declare function normalizeExecutorsCount(count: number): number;
export declare function subscribeAbort(signal: AbortSignal, callback: () => void): () => void;
export declare function reportPoolError(handler: (error: unknown) => void, error: unknown): void;
//# sourceMappingURL=pool-support.d.ts.map