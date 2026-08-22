import type { Context } from "./context.js";
export interface Lifecycle {
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
}
export type ComponentCategory = "dataSource" | "dataSink" | "storage" | "delayPool" | "taskPool" | "priorityTaskPool" | "component" | "httpServer" | "telemetry";
export interface RuntimeComponent {
    readonly category: ComponentCategory;
    readonly name: string;
    readonly lifecycle: Lifecycle;
}
//# sourceMappingURL=lifecycle.d.ts.map