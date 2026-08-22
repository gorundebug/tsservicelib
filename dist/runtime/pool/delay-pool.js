import { performance } from "node:perf_hooks";
import { err } from "../environment/index.js";
import { PoolStoppedError } from "./pool.js";
export class DelayPool {
    #name;
    #onError;
    #logger;
    #metrics;
    #tasks = new Set();
    #state = "created";
    #drain;
    #resolveDrain;
    constructor(options = {}) {
        this.#name = options.name ?? "delay";
        this.#onError = options.onError ?? (() => undefined);
        this.#logger = options.logger;
        this.#metrics = makeMetrics(options.metrics, options.service);
    }
    pendingCount() {
        return this.#tasks.size;
    }
    start(context) {
        void context;
        if (this.#state !== "created") {
            return Promise.reject(new Error(`pool ${this.#name} cannot start from ${this.#state}`));
        }
        this.#state = "running";
        return Promise.resolve();
    }
    delay(context, delayMs, execute) {
        if (context.cancelled()) {
            throw context.signal().reason ?? new Error("delay context is cancelled");
        }
        if (this.#state === "stopping" || this.#state === "stopped") {
            throw new PoolStoppedError(this.#name);
        }
        const remaining = context.remainingMs();
        const effectiveDelay = Math.max(0, Math.min(delayMs, remaining ?? Infinity));
        const task = {
            completed: false,
            timer: undefined,
            removeAbortListener: () => undefined
        };
        this.#metrics?.waitQueueLength.inc();
        const finish = (cancelled) => {
            if (task.completed) {
                return;
            }
            task.completed = true;
            if (task.timer !== undefined) {
                clearTimeout(task.timer);
            }
            task.removeAbortListener();
            const started = performance.now();
            let completion;
            try {
                completion = execute();
            }
            catch (error) {
                this.#onError(error);
                this.completeTask(task, context, started, cancelled);
                return;
            }
            void Promise.resolve(completion)
                .catch((error) => {
                this.#onError(error);
            })
                .finally(() => {
                this.completeTask(task, context, started, cancelled);
            });
        };
        const cancelled = () => {
            finish(true);
        };
        context.signal().addEventListener("abort", cancelled, { once: true });
        task.removeAbortListener = () => {
            context.signal().removeEventListener("abort", cancelled);
        };
        this.#tasks.add(task);
        if (effectiveDelay === 0) {
            queueMicrotask(() => {
                finish(false);
            });
        }
        else {
            task.timer = setTimeout(() => {
                finish(false);
            }, effectiveDelay);
        }
    }
    async stop(context) {
        if (this.#state === "stopped") {
            return;
        }
        if (this.#drain !== undefined) {
            await this.#drain;
            return;
        }
        this.#state = "stopping";
        if (this.#tasks.size === 0) {
            this.#state = "stopped";
            return;
        }
        this.#drain = new Promise((resolve) => {
            this.#resolveDrain = resolve;
        });
        const drain = this.#drain;
        if (context.cancelled()) {
            this.reportStopTimeout(context);
            await drain;
            return;
        }
        let removeAbortListener = () => undefined;
        const aborted = new Promise((resolve) => {
            const listener = () => {
                resolve("aborted");
            };
            context.signal().addEventListener("abort", listener, { once: true });
            removeAbortListener = () => {
                context.signal().removeEventListener("abort", listener);
            };
        });
        try {
            if ((await Promise.race([drain.then(() => "drained"), aborted])) === "aborted") {
                this.reportStopTimeout(context);
                await drain;
            }
        }
        finally {
            removeAbortListener();
        }
    }
    completeTask(task, context, started, cancelled) {
        this.#tasks.delete(task);
        this.#metrics?.waitQueueLength.dec();
        this.#metrics?.tasksTotal.inc(context);
        this.#metrics?.executionDuration.observe(context, (performance.now() - started) / 1_000);
        if (cancelled)
            this.#metrics?.taskCancelled.inc(context);
        this.finishDrainIfIdle();
    }
    reportStopTimeout(context) {
        const reason = context.signal().reason;
        const error = reason instanceof Error ? reason : new Error("delay pool stop timed out");
        this.#logger?.warn(context, "delay pool stopped by timeout", err(error));
        this.#metrics?.stopTimeout.inc(context);
    }
    finishDrainIfIdle() {
        if (this.#state !== "stopping" || this.#tasks.size !== 0) {
            return;
        }
        this.#state = "stopped";
        this.#resolveDrain?.();
        this.#resolveDrain = undefined;
    }
}
function makeMetrics(metrics, service) {
    if (metrics?.enabled() !== true || service === undefined)
        return undefined;
    const scope = metrics.scope("delay_pool", { service });
    const waitQueueLength = scope.gauge("wait_queue_length", "Delay pool wait queue length");
    waitQueueLength.set(0);
    const events = scope.counterVec("events_total", "Total number of events in delay pool");
    return {
        waitQueueLength,
        tasksTotal: scope.counter("tasks_total", "Total number of tasks executed by delay pool"),
        executionDuration: scope.histogram("task_execution_duration_seconds", "Task execution duration in seconds"),
        stopTimeout: events.with({ event: "stop_timeout" }),
        taskCancelled: events.with({ event: "task_cancelled" })
    };
}
//# sourceMappingURL=delay-pool.js.map