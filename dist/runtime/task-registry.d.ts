import { type Context } from "./context.js";
export type RuntimeTask<T> = (signal: AbortSignal) => Promise<T>;
export type RuntimeTaskErrorHandler = (error: Error, context?: Context) => void;
export declare class RuntimeTaskRegistry {
    #private;
    constructor(onError?: RuntimeTaskErrorHandler);
    accepting(): boolean;
    activeCount(): number;
    admit<T>(task: RuntimeTask<T>, externalSignal?: AbortSignal): Promise<T>;
    admitDetached<T>(task: RuntimeTask<T>, externalSignal?: AbortSignal): void;
    private startTask;
    stopAdmission(): void;
    admitDetachedWithContext<T>(context: Context, task: RuntimeTask<T>): void;
    cancel(reason?: unknown): void;
    drain(timeoutMs?: number): Promise<void>;
}
//# sourceMappingURL=task-registry.d.ts.map