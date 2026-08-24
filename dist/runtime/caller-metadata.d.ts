import type { Caller } from "./stream.js";
export type CallerType = "taskpool" | "prioritytaskpool" | "parallel" | "durable";
export interface CallerMetadata {
    readonly type: CallerType;
    readonly taskPoolName?: string | undefined;
}
export declare function setCallerMetadata<T>(caller: Caller<T>, value: CallerMetadata): Caller<T>;
export declare function callerMetadata<T>(caller: Caller<T>): CallerMetadata | undefined;
//# sourceMappingURL=caller-metadata.d.ts.map