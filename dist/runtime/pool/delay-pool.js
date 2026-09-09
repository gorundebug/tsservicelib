import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { IndexedHeap } from "./indexed-heap.js";
import { subscribeAbort, reportPoolError } from "./pool-support.js";
import { performance } from "node:perf_hooks";
import { err } from "../environment/index.js";
import { PoolStoppedError } from "./pool.js";
export class DelayPool {
    #schedule = AsyncLocalStorage.snapshot();
    #name;
    #onError;
    #logger;
    #metrics;
    #tasks = new Set();
    #queue = new IndexedHeap((a, b) => a.runAt - b.runAt || a.sequence - b.sequence);
    #timer;
    #armedAt;
    #sequence = 0;
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
        if (Number.isNaN(delayMs))
            throw new RangeError("delay must not be NaN");
        const remaining = context.remainingMs();
        const effectiveDelay = Math.max(0, Math.min(delayMs, remaining ?? Infinity));
        const task = {
            completed: false,
            runAt: performance.now() + effectiveDelay,
            sequence: this.#sequence++,
            context,
            execute: AsyncResource.bind(execute),
            expeditedByDeadline: remaining !== undefined && remaining < delayMs,
            removeAbortListener: () => undefined
        };
        this.#tasks.add(task);
        this.#metrics?.waitQueueLength.inc();
        if (effectiveDelay === 0) {
            this.dispatch(task, task.expeditedByDeadline);
            return;
        }
        this.#queue.push(task);
        task.removeAbortListener = subscribeAbort(context.signal(), () => {
            this.dispatch(task, true);
            this.arm();
        });
        this.arm();
    }
    arm() {
        const next = this.#queue.peek()?.runAt;
        if (next === this.#armedAt)
            return;
        if (this.#timer !== undefined)
            clearTimeout(this.#timer);
        this.#timer = undefined;
        this.#armedAt = next;
        if (next === undefined || next === Infinity)
            return;
        // Node otherwise turns delays above INT32_MAX into a one millisecond timer.
        this.#timer = this.#schedule(() => setTimeout(() => {
            this.#timer = undefined;
            this.#armedAt = undefined;
            const now = performance.now();
            for (;;) {
                const task = this.#queue.peek();
                if (task === undefined || task.runAt > now)
                    break;
                this.dispatch(task, task.expeditedByDeadline || task.context.cancelled());
            }
            this.arm();
        }, Math.min(2_147_483_647, Math.max(0, Math.ceil(next - performance.now())))));
    }
    dispatch(task, cancelled) {
        if (task.completed)
            return;
        task.completed = true;
        this.#queue.remove(task);
        task.removeAbortListener();
        // In particular, abort listeners must never invoke user code inline.
        queueMicrotask(() => {
            const started = performance.now();
            let completion;
            try {
                completion = task.execute();
            }
            catch (error) {
                reportPoolError(this.#onError, error);
                this.completeTask(task, task.context, started, cancelled);
                return;
            }
            void Promise.resolve(completion)
                .catch((error) => {
                reportPoolError(this.#onError, error);
            })
                .finally(() => {
                this.completeTask(task, task.context, started, cancelled);
            });
        });
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