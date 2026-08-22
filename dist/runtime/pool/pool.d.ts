import type { Completion } from "../stream.js";
import type { Logger, Metrics } from "../environment/index.js";
export declare class PoolStoppedError extends Error {
    constructor(name: string);
}
export type PoolTask = () => Completion;
export interface TaskPoolOptions {
    readonly name: string;
    readonly executorsCount: number;
    readonly onError?: (error: unknown) => void;
    readonly logger?: Logger;
    readonly metrics?: Metrics;
    readonly service?: string;
}
//# sourceMappingURL=pool.d.ts.map