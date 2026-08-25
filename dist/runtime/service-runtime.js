import { Context } from "./context.js";
import { err, str } from "./environment/index.js";
import { RuntimeStoppedError } from "./errors.js";
import { RuntimeTaskRegistry } from "./task-registry.js";
const START_ORDER = [
    "dataSource",
    "dataSink",
    "managedDataConnector",
    "storage",
    "delayPool",
    "taskPool",
    "priorityTaskPool",
    "component",
    "httpServer",
    "telemetry"
];
const ADMISSION_CATEGORIES = new Set([
    "dataSource",
    "managedDataConnector",
    "delayPool",
    "taskPool",
    "priorityTaskPool",
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
        if (this.#state === "starting") {
            this.#startupController.abort(new RuntimeStoppedError("runtime startup was stopped"));
            try {
                await this.#startPromise;
            }
            catch {
                // startOnce owns partial-start rollback and preserves its error for the start caller.
            }
        }
        if (this.#state === "created" || this.#state === "stopped") {
            this.#tasks.stopAdmission();
            this.#state = "stopped";
            return;
        }
        this.#state = "stopping";
        const admission = this.#started.filter((item) => ADMISSION_CATEGORIES.has(item.category));
        await this.stopAdmission(admission, context);
        try {
            await this.#tasks.drain(drainTimeoutMs);
            this.#tasks.stopAdmission();
        }
        catch (error) {
            this.#tasks.cancel(error);
            await this.#tasks.drain();
            throw error;
        }
        finally {
            await this.stopConcurrent(this.#started.filter((item) => item.category === "dataSink" ||
                item.category === "managedDataConnector" ||
                item.category === "storage"), context);
            await this.stopSequential(this.#started.filter((item) => item.category === "telemetry"), context);
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
        const results = await Promise.allSettled(components.toReversed().map(async (item) => item.lifecycle.stop(context)));
        for (const [index, result] of results.entries()) {
            if (result.status === "rejected") {
                const component = components[components.length - index - 1];
                if (component !== undefined)
                    this.logStopError(context, component, result.reason);
            }
        }
    }
    async stopAdmission(components, context) {
        const results = await Promise.allSettled(components.toReversed().map(async (item) => {
            if (item.category === "managedDataConnector" && "stopAdmission" in item.lifecycle) {
                await item.lifecycle.stopAdmission(context);
                return;
            }
            await item.lifecycle.stop(context);
        }));
        for (const [index, result] of results.entries()) {
            if (result.status === "rejected") {
                const component = components[components.length - index - 1];
                if (component !== undefined)
                    this.logStopError(context, component, result.reason);
            }
        }
    }
    async stopSequential(components, context) {
        for (const component of components.toReversed()) {
            try {
                await component.lifecycle.stop(context);
            }
            catch (error) {
                this.logStopError(context, component, error);
            }
        }
    }
    logStopError(context, component, error) {
        this.#environment
            .log()
            .warn(context, "runtime component shutdown", str("category", component.category), str("component", component.name), err(error instanceof Error ? error : new Error(String(error))));
    }
}
//# sourceMappingURL=service-runtime.js.map