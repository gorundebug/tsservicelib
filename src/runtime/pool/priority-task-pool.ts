import type { Context } from "../context.js";
import { err, int, str, type Logger } from "../environment/log.js";
import type { Lifecycle } from "../lifecycle.js";
import { PoolStoppedError, type PoolTask, type TaskPoolOptions } from "./pool.js";
import { awaitPoolDrain, makeTaskPoolMetrics, type TaskPoolMetrics } from "./task-pool-metrics.js";

interface PriorityTask {
  readonly context: Context;
  readonly execute: PoolTask;
  priority: number;
  readonly sequence: number;
  removeAbortListener(): void;
}

/** Minimum-priority-first task pool with FIFO ordering for equal priorities. */
export class PriorityTaskPool implements Lifecycle {
  readonly #name: string;
  readonly #onError: (error: unknown) => void;
  readonly #logger: Logger | undefined;
  readonly #metrics: TaskPoolMetrics | undefined;
  readonly #queue: PriorityTask[] = [];
  #executorsCount: number;
  #active = 0;
  #sequence = 0;
  #state: "created" | "running" | "stopping" | "stopped" = "created";
  #drain: Promise<void> | undefined;
  #resolveDrain: (() => void) | undefined;

  public constructor(options: TaskPoolOptions) {
    this.#name = options.name;
    this.#executorsCount = normalizeExecutorsCount(options.executorsCount);
    this.#onError = options.onError ?? (() => undefined);
    this.#logger = options.logger;
    this.#metrics = makeTaskPoolMetrics("priority", options);
  }

  public name(): string {
    return this.#name;
  }

  public executorsCount(): number {
    return this.#executorsCount;
  }

  public queueLength(): number {
    return this.#queue.length;
  }

  public activeCount(): number {
    return this.#active;
  }

  public resize(executorsCount: number): void {
    this.#executorsCount = normalizeExecutorsCount(executorsCount);
    if (this.#state === "running") {
      this.#metrics?.executorsTarget.set(this.#executorsCount);
      this.#metrics?.executorsAllocated.set(this.#executorsCount);
    }
    this.pump();
  }

  public start(context: Context): Promise<void> {
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

  public addTask(context: Context, priority: number, execute: PoolTask): void {
    if (context.cancelled()) {
      this.#metrics?.taskRejected.inc(context);
      throw context.signal().reason ?? new Error("task context is cancelled");
    }
    if (this.#state === "stopping" || this.#state === "stopped") {
      this.#metrics?.taskRejected.inc(context);
      throw new PoolStoppedError(this.#name);
    }

    const task: PriorityTask = {
      context,
      execute,
      priority,
      sequence: this.#sequence,
      removeAbortListener: () => undefined
    };
    this.#sequence += 1;
    const cancel = (): void => {
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

  public async stop(context: Context): Promise<void> {
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
      const reason = context.signal().reason as unknown;
      this.#logger?.warn(
        context,
        "priority task pool stopped by timeout",
        str("pool", this.#name),
        int("tasks_count", this.#queue.length),
        err(reason instanceof Error ? reason : new Error("timeout"))
      );
      this.#metrics?.stopTimeout.inc(context);
    });
  }

  private insert(task: PriorityTask): void {
    const index = this.#queue.findIndex(
      (item) =>
        item.priority > task.priority ||
        (item.priority === task.priority && item.sequence > task.sequence)
    );
    if (index === -1) {
      this.#queue.push(task);
    } else {
      this.#queue.splice(index, 0, task);
    }
  }

  private pump(): void {
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

  private run(task: PriorityTask): void {
    const started = performance.now();
    this.#metrics?.executorsBusy.inc();
    let completion: ReturnType<PoolTask>;
    try {
      completion = task.execute();
    } catch (error: unknown) {
      this.#onError(error);
      this.taskFinished(task.context, started);
      return;
    }
    void Promise.resolve(completion)
      .catch((error: unknown) => {
        this.#onError(error);
      })
      .finally(() => {
        this.taskFinished(task.context, started);
      });
  }

  private taskFinished(context: Context, started: number): void {
    this.#metrics?.executorsBusy.dec();
    this.#metrics?.tasksTotal.inc(context);
    this.#metrics?.executionDuration.observe(context, (performance.now() - started) / 1_000);
    this.#active -= 1;
    this.pump();
  }

  private finishDrainIfIdle(): void {
    if (this.#state !== "stopping" || this.#active !== 0 || this.#queue.length !== 0) {
      return;
    }
    this.#state = "stopped";
    this.#metrics?.executorsAllocated.set(0);
    this.#resolveDrain?.();
    this.#resolveDrain = undefined;
  }
}

function normalizeExecutorsCount(executorsCount: number): number {
  if (!Number.isInteger(executorsCount) || executorsCount < 1) {
    throw new RangeError("executorsCount must be a positive integer");
  }
  return executorsCount;
}
