import { err, str } from "./environment/index.js";
import { ContextDeadlineExceededError, RuntimeDrainTimeoutError, RuntimeStoppedError, errorFromUnknown } from "./errors.js";
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
    #admissionStops = new Map();
    #componentStops = new Map();
    #rollbackPending = false;
    #startupPending = false;
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
    start(context) {
        if (this.#state !== "created") {
            return Promise.reject(new Error(`runtime cannot start from state ${this.#state}`));
        }
        this.#state = "starting";
        this.#startupPending = true;
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
            this.#rollbackPending = true;
            try {
                await this.rollback(context);
            }
            catch {
                // Keep the original start error; unfinished rollback stays retryable.
            }
            this.#state =
                this.#started.length === 0 && this.#tasks.activeCount() === 0 ? "stopped" : "stopping";
            throw error;
        }
        finally {
            this.#startupPending = false;
        }
    }
    async stop(context, drainTimeoutMs) {
        const stopContext = drainTimeoutMs === undefined ? context : context.bounded(Math.max(0, drainTimeoutMs));
        const budget = stopContext.remainingMs();
        // There is nothing to wait for, even if this caller is already cancelled.
        if (!this.#startupPending && this.#started.length === 0 && this.#tasks.activeCount() === 0) {
            this.#tasks.stopAdmission();
            this.#state = "stopped";
            return;
        }
        for (;;) {
            const ownsAttempt = this.#stopPromise === undefined;
            if (ownsAttempt) {
                const attempt = this.stopOnce(stopContext);
                this.#stopPromise = attempt;
                const release = () => {
                    if (this.#stopPromise === attempt)
                        this.#stopPromise = undefined;
                };
                void attempt.then(release, release);
            }
            const attempt = this.#stopPromise;
            if (attempt === undefined)
                continue;
            const [result] = await settleWithinDeadline(stopContext, [attempt]);
            if (result === undefined) {
                // Publish the failed wait only after sealing new task admission. Running
                // operations retain their ownership and can still be awaited by a retry.
                this.#tasks.stopAdmission();
                throw this.shutdownError(stopContext, budget);
            }
            if (result.status === "fulfilled")
                return;
            if (stopContext.cancelled() || stopContext.remainingMs() === 0) {
                this.#tasks.stopAdmission();
                throw this.shutdownError(stopContext, budget);
            }
            if (ownsAttempt) {
                this.#tasks.stopAdmission();
                throw errorFromUnknown(result.reason);
            }
            // The preceding caller's context may have expired. This caller can retry
            // with its own context, without duplicating still-running component stops.
        }
    }
    async stopOnce(context) {
        this.#state = "stopping";
        if (this.#startupPending) {
            this.#startupController.abort(new RuntimeStoppedError("runtime startup was stopped"));
            const budget = context.remainingMs();
            const [startup] = await settleWithinDeadline(context, [
                this.#startPromise ?? Promise.resolve()
            ]);
            if (startup === undefined) {
                const error = this.shutdownError(context, budget);
                this.#tasks.cancel(error);
                this.#environment.log().warn(context, "runtime startup shutdown timed out");
                throw error;
            }
        }
        if (this.#rollbackPending) {
            await this.rollback(context);
            await this.drainTasks(context);
            this.#state = "stopped";
            return;
        }
        await this.stopAdmission(this.#started.filter((item) => ADMISSION_CATEGORIES.has(item.category)), context);
        // Drain ordinary graph producers while outbound managed clients are alive.
        await this.stopConcurrent(this.#started.filter((item) => ADMISSION_CATEGORIES.has(item.category) && "stopAdmission" in item.lifecycle), context);
        await this.stopAdmission(this.#started.filter((item) => item.category === "managedDataConnector"), context);
        await this.stopConcurrent(this.#started.filter((item) => item.category === "storage" ||
            item.category === "delayPool" ||
            item.category === "taskPool" ||
            item.category === "priorityTaskPool"), context);
        await this.drainTasks(context);
        await this.stopConcurrent(this.#started.filter((item) => item.category === "dataSink"), context);
        await this.stopConcurrent(this.#started.filter((item) => item.category === "managedDataConnector"), context);
        await this.stopSequential(this.#started.filter((item) => item.category === "telemetry"), context);
        this.#state = "stopped";
    }
    async drainTasks(context) {
        try {
            if (this.#tasks.activeCount() > 0) {
                const budget = context.remainingMs();
                const [result] = await settleWithinDeadline(context, [this.#tasks.drain(budget)]);
                if (result === undefined)
                    throw this.shutdownError(context, budget);
                if (result.status === "rejected")
                    throw errorFromUnknown(result.reason);
            }
            this.#tasks.stopAdmission();
        }
        catch (error) {
            this.#tasks.cancel(error);
            throw error;
        }
    }
    async rollback(context) {
        await this.stopSequential([...this.#started], context);
        this.#rollbackPending = false;
    }
    stopComponent(component, context, admission) {
        const admissionOnly = admission && "stopAdmission" in component.lifecycle;
        const pending = admissionOnly ? this.#admissionStops : this.#componentStops;
        const existing = pending.get(component);
        if (existing !== undefined) {
            if (existing.context === context)
                return existing.promise;
            return existing.promise.catch((error) => {
                const reason = existing.context.signal().reason;
                const previousWaitExpired = existing.context.cancelled() || existing.context.remainingMs() === 0;
                const cancellationError = error instanceof RuntimeDrainTimeoutError ||
                    error instanceof ContextDeadlineExceededError ||
                    (reason !== undefined && error === reason) ||
                    (error instanceof Error && reason !== undefined && error.cause === reason);
                if (previousWaitExpired &&
                    cancellationError &&
                    !context.cancelled() &&
                    context.remainingMs() !== 0) {
                    // Only the previous caller's wait failed. The component still owns
                    // its physical drain; join it again using this caller's live context.
                    return this.stopComponent(component, context, admission);
                }
                throw error;
            });
        }
        const operation = Promise.resolve()
            .then(async () => {
            if (admissionOnly)
                await component.lifecycle.stopAdmission(context);
            else
                await component.lifecycle.stop(context);
        })
            .then(() => {
            if (!admissionOnly) {
                const index = this.#started.indexOf(component);
                if (index >= 0)
                    this.#started.splice(index, 1);
                this.#admissionStops.delete(component);
            }
        }, (error) => {
            if (pending.get(component)?.promise === operation)
                pending.delete(component);
            throw error;
        });
        pending.set(component, { context, promise: operation });
        return operation;
    }
    async stopConcurrent(components, context) {
        await this.stopGroup(components, context, false);
    }
    async stopAdmission(components, context) {
        await this.stopGroup(components, context, true);
    }
    async stopGroup(components, context, admission) {
        const ordered = components.toReversed();
        const budget = context.remainingMs();
        const results = await settleWithinDeadline(context, ordered.map((component) => this.stopComponent(component, context, admission)));
        let failure;
        for (const [index, component] of ordered.entries()) {
            const result = results[index];
            if (result === undefined) {
                this.logStopTimeout(context, component);
                failure ??= this.shutdownError(context, budget);
            }
            else if (result.status === "rejected") {
                this.logStopError(context, component, result.reason);
                failure ??= errorFromUnknown(result.reason);
            }
        }
        if (failure !== undefined)
            throw failure;
    }
    async stopSequential(components, context) {
        for (const component of components.toReversed()) {
            const budget = context.remainingMs();
            const [result] = await settleWithinDeadline(context, [
                this.stopComponent(component, context, false)
            ]);
            if (result === undefined) {
                this.logStopTimeout(context, component);
                throw this.shutdownError(context, budget);
            }
            if (result.status === "rejected") {
                this.logStopError(context, component, result.reason);
                throw errorFromUnknown(result.reason);
            }
        }
    }
    shutdownError(context, budget) {
        const reason = context.signal().reason;
        if (reason instanceof ContextDeadlineExceededError || context.remainingMs() === 0) {
            return new RuntimeDrainTimeoutError(budget ?? 0);
        }
        return errorFromUnknown(reason ?? new RuntimeStoppedError("runtime shutdown was cancelled"));
    }
    logStopTimeout(context, component) {
        this.#environment
            .log()
            .warn(context, "runtime component shutdown timed out", str("category", component.category), str("component", component.name));
    }
    logStopError(context, component, error) {
        this.#environment
            .log()
            .warn(context, "runtime component shutdown", str("category", component.category), str("component", component.name), err(errorFromUnknown(error)));
    }
}
//# sourceMappingURL=service-runtime.js.map