import { Context } from "./context.js";
import { err, str } from "./environment/index.js";
import { RuntimeDrainTimeoutError, RuntimeStoppedError, errorFromUnknown } from "./errors.js";
import { settleWithinDeadline } from "./lifecycle.js";
import { RuntimeTaskRegistry } from "./task-registry.js";
const START_ORDER = [
    "telemetry",
    "managedDataConnector",
    "storage",
    "delayPool",
    "taskPool",
    "priorityTaskPool",
    "component",
    "dataSink",
    "dataSource",
    "httpServer"
];
const ADMISSION_CATEGORIES = new Set([
    "dataSource",
    "component",
    "httpServer"
]);
export class ServiceRuntime {
    #components = [];
    #environment;
    #startupController = new AbortController();
    #started = [];
    #tasks;
    #startPromise;
    #state = "created";
    #stopPromise;
    constructor(environment, tasks = new RuntimeTaskRegistry()) {
        this.#environment = environment;
        this.#tasks = tasks;
    }
    tasks() {
        return this.#tasks;
    }
    state() {
        return this.#state;
    }
    register(component) {
        if (this.#state !== "created") {
            throw new Error("runtime components must be registered before start");
        }
        if (this.#components.some(({ category, name }) => category === component.category && name === component.name)) {
            throw new Error(`duplicate runtime component ${component.category}:${component.name}`);
        }
        this.#components.push(component);
    }
    start(context = Context.background()) {
        if (this.#state !== "created") {
            return Promise.reject(new Error(`runtime cannot start from state ${this.#state}`));
        }
        this.#state = "starting";
        const startupContext = context.withExternalCancellation(this.#startupController.signal);
        this.#startPromise = this.startOnce(startupContext);
        return this.#startPromise;
    }
    async startOnce(context) {
        try {
            await this.#environment.buildRuntimeStreams();
            context.signal().throwIfAborted();
            this.#environment.validateRuntimeTopology();
            for (const category of START_ORDER) {
                if (category === "dataSource") {
                    for (const component of this.#started.filter((item) => item.category === "managedDataConnector")) {
                        await component.lifecycle.startAdmission(context);
                        context.signal().throwIfAborted();
                    }
                }
                for (const component of this.#components.filter((item) => item.category === category)) {
                    context.signal().throwIfAborted();
                    await component.lifecycle.start(context);
                    this.#started.push(component);
                    context.signal().throwIfAborted();
                }
            }
            this.#state = "running";
        }
        catch (error) {
            this.#tasks.cancel(error);
            await this.rollback(context.withoutCancellation());
            this.#state = "stopped";
            throw error;
        }
    }
    stop(context = Context.background(), drainTimeoutMs) {
        if (this.#stopPromise !== undefined) {
            return this.#stopPromise;
        }
        this.#stopPromise = this.stopOnce(context, drainTimeoutMs);
        return this.#stopPromise;
    }
    async stopOnce(context, drainTimeoutMs) {
        const stopContext = drainTimeoutMs === undefined ? context : context.bounded(Math.max(0, drainTimeoutMs));
        if (this.#state === "starting") {
            this.#startupController.abort(new RuntimeStoppedError("runtime startup was stopped"));
            const [startup] = await settleWithinDeadline(stopContext, [
                this.#startPromise ?? Promise.resolve()
            ]);
            if (startup === undefined) {
                // startOnce still owns partial-start rollback and the dependencies of
                // the uncooperative initializer. Do not stop those objects underneath it.
                this.#tasks.cancel(stopContext.signal().reason ??
                    new RuntimeStoppedError("runtime startup shutdown timed out"));
                this.#state = "stopped";
                this.#environment.log().warn(stopContext, "runtime startup shutdown timed out");
                return;
            }
        }
        if (this.#state === "created" || this.#state === "stopped") {
            this.#tasks.stopAdmission();
            this.#state = "stopped";
            return;
        }
        this.#state = "stopping";
        const admission = this.#started.filter((item) => ADMISSION_CATEGORIES.has(item.category));
        await this.stopAdmission(admission, stopContext);
        // A source can be awaiting work submitted through a managed connector.
        // Drain ordinary sources first (Cron -> Temporal is the canonical case),
        // then stop durable worker admission while outbound clients remain alive.
        await this.stopConcurrent(this.#started.filter((item) => item.category === "dataSource" && "stopAdmission" in item.lifecycle), stopContext);
        await this.stopAdmission(this.#started.filter((item) => item.category === "managedDataConnector"), stopContext);
        try {
            // Pools and timers can create ParallelCall tasks while draining. Stop
            // those graph-work producers before observing the shared task registry.
            await this.stopConcurrent(this.#started.filter((item) => item.category === "storage" ||
                item.category === "delayPool" ||
                item.category === "taskPool" ||
                item.category === "priorityTaskPool"), stopContext);
            if (this.#tasks.activeCount() > 0) {
                const drainBudget = stopContext.remainingMs();
                const [drained] = await settleWithinDeadline(stopContext, [this.#tasks.drain(drainBudget)]);
                if (drained === undefined) {
                    if (drainBudget !== undefined && stopContext.remainingMs() === 0) {
                        throw new RuntimeDrainTimeoutError(drainBudget);
                    }
                    throw errorFromUnknown(stopContext.signal().reason ?? new RuntimeStoppedError("runtime shutdown was cancelled"));
                }
                if (drained.status === "rejected")
                    throw errorFromUnknown(drained.reason);
            }
            this.#tasks.stopAdmission();
        }
        catch (error) {
            this.#tasks.cancel(error);
            throw error;
        }
        finally {
            await this.stopConcurrent(this.#started.filter((item) => item.category === "dataSink"), stopContext);
            await this.stopConcurrent(this.#started.filter((item) => item.category === "managedDataConnector"), stopContext);
            await this.stopSequential(this.#started.filter((item) => item.category === "telemetry"), stopContext);
            this.#started.length = 0;
            this.#state = "stopped";
        }
    }
    async rollback(context) {
        for (const component of this.#started.toReversed()) {
            try {
                await component.lifecycle.stop(context);
            }
            catch (error) {
                // Preserve the original startup failure while still attempting every rollback.
                this.logStopError(context, component, error);
            }
        }
        this.#started.length = 0;
    }
    async stopConcurrent(components, context) {
        const ordered = components.toReversed();
        const results = await settleWithinDeadline(context, ordered.map(async (item) => item.lifecycle.stop(context)));
        for (const [index, result] of results.entries()) {
            const component = ordered[index];
            if (component === undefined)
                continue;
            if (result === undefined) {
                this.logStopTimeout(context, component);
                continue;
            }
            if (result.status === "rejected") {
                this.logStopError(context, component, result.reason);
            }
        }
    }
    async stopAdmission(components, context) {
        const ordered = components.toReversed();
        const results = await settleWithinDeadline(context, ordered.map(async (item) => {
            if ("stopAdmission" in item.lifecycle) {
                await item.lifecycle.stopAdmission(context);
                return;
            }
            await item.lifecycle.stop(context);
        }));
        for (const [index, result] of results.entries()) {
            const component = ordered[index];
            if (component === undefined)
                continue;
            if (result === undefined) {
                this.logStopTimeout(context, component);
                continue;
            }
            if (result.status === "rejected") {
                this.logStopError(context, component, result.reason);
            }
        }
    }
    async stopSequential(components, context) {
        for (const component of components.toReversed()) {
            const [result] = await settleWithinDeadline(context, [component.lifecycle.stop(context)]);
            if (result === undefined) {
                this.logStopTimeout(context, component);
            }
            else if (result.status === "rejected") {
                this.logStopError(context, component, result.reason);
            }
        }
    }
    logStopTimeout(context, component) {
        this.#environment
            .log()
            .warn(context, "runtime component shutdown timed out", str("category", component.category), str("component", component.name));
    }
    logStopError(context, component, error) {
        this.#environment
            .log()
            .warn(context, "runtime component shutdown", str("category", component.category), str("component", component.name), err(error instanceof Error ? error : new Error(String(error))));
    }
}
//# sourceMappingURL=service-runtime.js.map