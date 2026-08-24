import { Context } from "./context.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import type { RuntimeComponent } from "./lifecycle.js";
import { RuntimeTaskRegistry } from "./task-registry.js";
export declare class ServiceRuntime {
    #private;
    constructor(environment: RuntimeEnvironment, tasks?: RuntimeTaskRegistry);
    tasks(): RuntimeTaskRegistry;
    state(): "created" | "starting" | "running" | "stopping" | "stopped";
    register(component: RuntimeComponent): void;
    start(context?: Context): Promise<void>;
    private startOnce;
    stop(context?: Context, drainTimeoutMs?: number): Promise<void>;
    private stopOnce;
    private rollback;
    private stopConcurrent;
    private stopAdmission;
    private stopSequential;
    private logStopError;
}
//# sourceMappingURL=service-runtime.d.ts.map