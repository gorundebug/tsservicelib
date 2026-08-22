import type { Context } from "../context.js";
import { type Logger, type Metrics } from "../environment/index.js";
import type { Lifecycle } from "../lifecycle.js";
import type { Completion } from "../stream.js";
export interface DelayPoolOptions {
    readonly name?: string;
    readonly onError?: (error: unknown) => void;
    readonly logger?: Logger;
    readonly metrics?: Metrics;
    readonly service?: string;
}
export declare class DelayPool implements Lifecycle {
    #private;
    constructor(options?: DelayPoolOptions);
    pendingCount(): number;
    start(context: Context): Promise<void>;
    delay(context: Context, delayMs: number, execute: () => Completion): void;
    stop(context: Context): Promise<void>;
    private completeTask;
    private reportStopTimeout;
    private finishDrainIfIdle;
}
//# sourceMappingURL=delay-pool.d.ts.map