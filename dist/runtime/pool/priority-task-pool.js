import { performance } from "node:perf_hooks";
import { err, int, str } from "../environment/index.js";
import { PoolStoppedError } from "./pool.js";
import { awaitPoolDrain, makeTaskPoolMetrics } from "./task-pool-metrics.js";
/** Minimum-priority-first task pool with FIFO ordering for equal priorities. */
export class PriorityTaskPool {
    #name;
    #onError;
    #logger;
    #metrics;
    #queue = [];
    #executorsCount;
    #active = 0;
    #sequence = 0;
    #state = "created";
    #drain;
    #resolveDrain;
    constructor(options) {
        this.#name = options.name;
        this.#executorsCount = normalizeExecutorsCount(options.executorsCount);
        this.#onError = options.onError ?? (() => undefined);
        this.#logger = options.logger;
        this.#metrics = makeTaskPoolMetrics("priority", options);
    }
    name() {
        return this.#name;
    }
    executorsCount() {
        return this.#executorsCount;
    }
    queueLength() {
        return this.#queue.length;
    }
    activeCount() {
        return this.#active;
    }
    resize(executorsCount) {
        this.#executorsCount = normalizeExecutorsCount(executorsCount);
        if (this.#state === "running") {
            this.#metrics?.executorsTarget.set(this.#executorsCount);
            this.#metrics?.executorsAllocated.set(this.#executorsCount);
        }
        this.pump();
    }
    start(context) {
        void context;
        if (this.#state !== "created") {
            return Promise.reject(new Error(`pool ${this.#name} cannot start from ${this.#state}`));
        }
        this.#state = "running";
        this.#metrics?.executorsTarget.set(this.#executorsCount);
        this.#metrics?.executorsAllocated.set(this.#executorsCount);
        this.pump();
        return Promise.resolve();
    }
    addTask(context, priority, execute) {
        if (context.cancelled()) {
            this.#metrics?.taskRejected.inc(context);
            throw context.signal().reason ?? new Error("task context is cancelled");
        }
        if (this.#state === "stopping" || this.#state === "stopped") {
            this.#metrics?.taskRejected.inc(context);
            throw new PoolStoppedError(this.#name);
        }
        const task = {
            context,
            execute,
            priority,
            sequence: this.#sequence,
            removeAbortListener: () => undefined
        };
        this.#sequence += 1;
        const cancel = () => {
            const index = this.#queue.indexOf(task);
            if (index >= 0) {
                this.#queue.splice(index, 1);
                task.priority = Number.NEGATIVE_INFINITY;
                this.insert(task);
                this.#metrics?.taskCancelledOrExpired.inc(context);
            }
            this.pump();
        };
        context.signal().addEventListener("abort", cancel, { once: true });
        task.removeAbortListener = () => {
            context.signal().removeEventListener("abort", cancel);
        };
        this.insert(task);
        this.#metrics?.queueLength.inc();
        this.pump();
    }
    async stop(context) {
        if (this.#state === "stopped") {
            return;
        }
        if (this.#drain !== undefined) {
            await this.#drain;
            return;
        }
        if (this.#state === "created") {
            for (const task of this.#queue.splice(0)) {
                task.removeAbortListener();
                this.#metrics?.queueLength.dec();
            }
            this.#state = "stopped";
            this.#metrics?.executorsAllocated.set(0);
            return;
        }
        this.#state = "stopping";
        this.#drain = new Promise((resolve) => {
            this.#resolveDrain = resolve;
        });
        this.pump();
        this.finishDrainIfIdle();
        await awaitPoolDrain(this.#drain, context, () => {
            const reason = context.signal().reason;
            this.#logger?.warn(context, "priority task pool stopped by timeout", str("pool", this.#name), int("tasks_count", this.#queue.length), err(reason instanceof Error ? reason : new Error("timeout")));
            this.#metrics?.stopTimeout.inc(context);
        });
    }
    insert(task) {
        const index = this.#queue.findIndex((item) => item.priority > task.priority ||
            (item.priority === task.priority && item.sequence > task.sequence));
        if (index === -1) {
            this.#queue.push(task);
        }
        else {
            this.#queue.splice(index, 0, task);
        }
    }
    pump() {
        if (this.#state !== "running" && this.#state !== "stopping") {
            return;
        }
        while (this.#active < this.#executorsCount && this.#queue.length > 0) {
            const task = this.#queue.shift();
            if (task === undefined) {
                break;
            }
            task.removeAbortListener();
            this.#metrics?.queueLength.dec();
            this.#active += 1;
            queueMicrotask(() => {
                this.run(task);
            });
        }
        this.finishDrainIfIdle();
    }
    run(task) {
        const started = performance.now();
        this.#metrics?.executorsBusy.inc();
        let completion;
        try {
            completion = task.execute();
        }
        catch (error) {
            this.#onError(error);
            this.taskFinished(task.context, started);
            return;
        }
        void Promise.resolve(completion)
            .catch((error) => {
            this.#onError(error);
        })
            .finally(() => {
            this.taskFinished(task.context, started);
        });
    }
    taskFinished(context, started) {
        this.#metrics?.executorsBusy.dec();
        this.#metrics?.tasksTotal.inc(context);
        this.#metrics?.executionDuration.observe(context, (performance.now() - started) / 1_000);
        this.#active -= 1;
        this.pump();
    }
    finishDrainIfIdle() {
        if (this.#state !== "stopping" || this.#active !== 0 || this.#queue.length !== 0) {
            return;
        }
        this.#state = "stopped";
        this.#metrics?.executorsAllocated.set(0);
        this.#resolveDrain?.();
        this.#resolveDrain = undefined;
    }
}
function normalizeExecutorsCount(executorsCount) {
    if (!Number.isInteger(executorsCount) || executorsCount < 1) {
        throw new RangeError("executorsCount must be a positive integer");
    }
    return executorsCount;
}
//# sourceMappingURL=priority-task-pool.js.map