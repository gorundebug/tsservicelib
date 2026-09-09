import { IndexedHeap } from "./indexed-heap.js";
import {
  bindPoolTask,
  normalizeExecutorsCount,
  subscribeAbort,
  reportPoolError
} from "./pool-support.js";
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
  readonly #queue = new IndexedHeap<PriorityTask>((a, b) =>
    a.priority < b.priority ? -1 : a.priority > b.priority ? 1 : a.sequence - b.sequence
  );
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
    if (this.#state === "stopping" || this.#state === "stopped") return;
    this.#executorsCount = normalizeExecutorsCount(executorsCount);
    if (this.#state === "running") {
      this.#metrics?.executorsTarget.set(this.#executorsCount);
      this.#metrics?.executorsAllocated.set(Math.max(this.#executorsCount, this.#active));
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
    this.#metrics?.executorsAllocated.set(Math.max(this.#executorsCount, this.#active));
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
      execute: bindPoolTask(execute),
      priority,
      sequence: this.#sequence,
      removeAbortListener: () => undefined
    };
    this.#sequence += 1;
    const cancel = (): void => {
      if (this.#queue.has(task)) {
        task.priority = Number.NEGATIVE_INFINITY;
        this.#queue.fix(task);
        this.#metrics?.taskCancelledOrExpired.inc(context);
      }
      this.pump();
    };
    task.removeAbortListener = subscribeAbort(context.signal(), cancel);
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
    this.#queue.push(task);
  }

  private pump(): void {
    if (this.#state !== "running" && this.#state !== "stopping") {
      return;
    }
    while (this.#active < this.#executorsCount && this.#queue.length > 0) {
      const task = this.#queue.pop();
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
      reportPoolError(this.#onError, error);
      this.taskFinished(task.context, started);
      return;
    }
    void Promise.resolve(completion)
      .catch((error: unknown) => {
        reportPoolError(this.#onError, error);
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
    this.#metrics?.executorsAllocated.set(Math.max(this.#executorsCount, this.#active));
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
