import type { Context } from "./context.js";
export interface Lifecycle {
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
}
export interface AdmissionLifecycle extends Lifecycle {
    stopAdmission(context: Context): Promise<void>;
}
export type ComponentCategory = "dataSource" | "dataSink" | "managedDataConnector" | "storage" | "delayPool" | "taskPool" | "priorityTaskPool" | "component" | "httpServer" | "telemetry";
export interface RuntimeComponent {
    readonly category: ComponentCategory;
    readonly name: string;
    readonly lifecycle: Lifecycle;
}
/** Observe every operation, but stop waiting when the shared shutdown context expires.
 * An undefined result means the operation still owns its resources and may finish later.
 */
export declare function settleWithinDeadline<T>(context: Context, operations: readonly Promise<T>[]): Promise<readonly (PromiseSettledResult<T> | undefined)[]>;
//# sourceMappingURL=lifecycle.d.ts.map