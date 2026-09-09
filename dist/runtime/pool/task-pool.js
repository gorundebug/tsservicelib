import { FifoQueue } from "./fifo-queue.js";
import { bindPoolTask, normalizeExecutorsCount, subscribeAbort, reportPoolError } from "./pool-support.js";
import { err, int, str } from "../environment/log.js";
import { PoolStoppedError } from "./pool.js";
import { awaitPoolDrain, makeTaskPoolMetrics } from "./task-pool-metrics.js";
/** Canonical unbounded FIFO task pool. Queue capacity is not backpressure. */
export class TaskPool {
    #name;
    #onError;
    #logger;
    #metrics;
    #queue = new FifoQueue();
    #executorsCount;
    #active = 0;
    #state = "created";
    #drain;
    #resolveDrain;
    constructor(options) {
        this.#name = options.name;
        this.#executorsCount = normalizeExecutorsCount(options.executorsCount);
        this.#onError = options.onError ?? (() => undefined);
        this.#logger = options.logger;
        this.#metrics = makeTaskPoolMetrics("task", options);
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
        if (this.#state === "stopping" || this.#state === "stopped")
            return;
        this.#executorsCount = normalizeExecutorsCount(executorsCount);
        if (this.#state === "running") {
            this.#metrics?.executorsTarget.set(this.#executorsCount);
            this.#metrics?.executorsAllocated.set(Math.max(this.#executorsCount, this.#active));
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
        this.#metrics?.executorsAllocated.set(Math.max(this.#executorsCount, this.#active));
        this.pump();
        return Promise.resolve();
    }
    addTask(context, execute) {
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
            execute: bindPoolTask(execute),
            removeAbortListener: () => undefined
        };
        const cancel = () => {
            if (this.#queue.moveToFront(task)) {
                this.#metrics?.taskCancelledOrExpired.inc(context);
            }
            this.pump();
        };
        task.removeAbortListener = subscribeAbort(context.signal(), cancel);
        this.#queue.push(task);
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
        this.#state = "stopping";
        this.#drain = new Promise((resolve) => {
            this.#resolveDrain = resolve;
        });
        this.pump();
        this.finishDrainIfIdle();
        await awaitPoolDrain(this.#drain, context, () => {
            const reason = context.signal().reason;
            this.#logger?.warn(context, "task pool stopped by timeout", str("pool", this.#name), int("tasks_count", this.#queue.length), err(reason instanceof Error ? reason : new Error("timeout")));
            this.#metrics?.stopTimeout.inc(context);
        });
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
            reportPoolError(this.#onError, error);
            this.taskFinished(task.context, started);
            return;
        }
        void Promise.resolve(completion)
            .catch((error) => {
            reportPoolError(this.#onError, error);
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
        this.#metrics?.executorsAllocated.set(Math.max(this.#executorsCount, this.#active));
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
//# sourceMappingURL=task-pool.js.map