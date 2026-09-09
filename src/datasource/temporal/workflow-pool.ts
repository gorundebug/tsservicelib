import { FifoQueue } from "../../runtime/pool/fifo-queue.js";
import { IndexedHeap } from "../../runtime/pool/indexed-heap.js";
import { subscribeAbort, reportPoolError } from "../../runtime/pool/pool-support.js";
import type { Context } from "../../runtime/context.js";
import type { Metrics } from "../../runtime/environment/metrics/metrics.js";
import {
  PoolStoppedError,
  type PoolTask,
  type PriorityTaskPoolLike,
  type TaskPoolLike
} from "../../runtime/pool/pool.js";
import { makeTaskPoolMetrics, type TaskPoolMetrics } from "../../runtime/pool/task-pool-metrics.js";

interface WorkflowTask {
  readonly context: Context;
  readonly execute: PoolTask;
  priority: number;
  readonly sequence: number;
  removeAbortListener(): void;
}

class WorkflowPoolCore {
  readonly #name: string;
  readonly #executors: number;
  readonly #onError: (error: unknown) => void;
  readonly #queue: FifoQueue<WorkflowTask> | IndexedHeap<WorkflowTask>;
  readonly #metrics: TaskPoolMetrics | undefined;
  #active = 0;
  #sequence = 0;
  #state: "created" | "running" | "stopping" | "stopped" = "created";
  #resolveDrain: (() => void) | undefined;
  #drain: Promise<void> | undefined;
  #resolveIdle: (() => void) | undefined;
  #idle: Promise<void> = Promise.resolve();

  public constructor(
    name: string,
    executors: number,
    priority: boolean,
    metrics: Metrics,
    service: string,
    onError: (error: unknown) => void
  ) {
    if (!Number.isSafeInteger(executors) || executors < 0) {
      throw new RangeError("executors must be a non-negative integer");
    }
    // Workflow replay must not depend on the host CPU count.
    executors = executors || 1;
    this.#queue = priority
      ? new IndexedHeap<WorkflowTask>((a, b) => a.priority - b.priority || a.sequence - b.sequence)
      : new FifoQueue<WorkflowTask>();
    this.#name = name;
    this.#executors = executors;
    this.#onError = onError;
    this.#metrics = makeTaskPoolMetrics(priority ? "priority" : "task", {
      name,
      executorsCount: executors,
      metrics,
      service
    });
  }

  public name(): string {
    return this.#name;
  }

  public executorsCount(): number {
    return this.#executors;
  }

  public queueLength(): number {
    return this.#queue.length;
  }

  public activeCount(): number {
    return this.#active;
  }

  public start(): void {
    if (this.#state !== "created") {
      throw new Error(`workflow pool ${this.#name} cannot start from ${this.#state}`);
    }
    this.#state = "running";
    this.#metrics?.executorsTarget.set(this.#executors);
    this.#metrics?.executorsAllocated.set(this.#executors);
    this.pump();
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
    if (this.#active === 0 && this.#queue.length === 0) {
      this.#idle = new Promise<void>((resolve) => {
        this.#resolveIdle = resolve;
      });
    }
    const task: WorkflowTask = {
      context,
      execute,
      priority,
      sequence: this.#sequence,
      removeAbortListener: () => undefined
    };
    this.#sequence += 1;
    const cancel = (): void => {
      let moved = false;
      if (this.#queue instanceof IndexedHeap) {
        if (this.#queue.has(task)) {
          task.priority = Number.NEGATIVE_INFINITY;
          this.#queue.fix(task);
          moved = true;
        }
      } else {
        moved = this.#queue.moveToFront(task);
      }
      if (moved) this.#metrics?.taskCancelledOrExpired.inc(context);
      this.pump();
    };
    task.removeAbortListener = subscribeAbort(context.signal(), cancel);
    this.insert(task);
    this.#metrics?.queueLength.inc();
    this.pump();
  }

  public async stop(): Promise<void> {
    if (this.#state === "stopped") return;
    if (this.#drain !== undefined) return this.#drain;
    this.#state = "stopping";
    this.#drain = new Promise<void>((resolve) => {
      this.#resolveDrain = resolve;
    });
    this.pump();
    this.finishDrainIfIdle();
    await this.#drain;
  }

  public async waitIdle(): Promise<void> {
    await this.#idle;
  }

  private pump(): void {
    if (this.#state !== "running" && this.#state !== "stopping") return;
    while (this.#active < this.#executors && this.#queue.length > 0) {
      const task = this.#queue instanceof IndexedHeap ? this.#queue.pop() : this.#queue.shift();
      if (task === undefined) break;
      task.removeAbortListener();
      this.#metrics?.queueLength.dec();
      this.#active += 1;
      void Promise.resolve().then(() => this.run(task));
    }
    this.finishDrainIfIdle();
  }

  private async run(task: WorkflowTask): Promise<void> {
    const started = Date.now();
    this.#metrics?.executorsBusy.inc();
    try {
      await task.execute();
    } catch (error: unknown) {
      reportPoolError(this.#onError, error);
    } finally {
      this.#metrics?.executorsBusy.dec();
      this.#metrics?.tasksTotal.inc(task.context);
      this.#metrics?.executionDuration.observe(task.context, (Date.now() - started) / 1_000);
      this.#active -= 1;
      this.pump();
    }
  }

  private insert(task: WorkflowTask): void {
    this.#queue.push(task);
  }

  private finishDrainIfIdle(): void {
    if (this.#active !== 0 || this.#queue.length !== 0) return;
    this.#resolveIdle?.();
    this.#resolveIdle = undefined;
    if (this.#state !== "stopping") return;
    this.#state = "stopped";
    this.#metrics?.queueLength.set(0);
    this.#metrics?.executorsBusy.set(0);
    this.#metrics?.executorsAllocated.set(0);
    this.#resolveDrain?.();
    this.#resolveDrain = undefined;
  }
}

export class WorkflowTaskPool implements TaskPoolLike {
  readonly #core: WorkflowPoolCore;

  public constructor(
    name: string,
    executors: number,
    metrics: Metrics,
    service: string,
    onError: (error: unknown) => void
  ) {
    this.#core = new WorkflowPoolCore(name, executors, false, metrics, service, onError);
  }

  public name(): string {
    return this.#core.name();
  }
  public executorsCount(): number {
    return this.#core.executorsCount();
  }
  public queueLength(): number {
    return this.#core.queueLength();
  }
  public activeCount(): number {
    return this.#core.activeCount();
  }
  public start(): void {
    this.#core.start();
  }
  public stop(): Promise<void> {
    return this.#core.stop();
  }
  public waitIdle(): Promise<void> {
    return this.#core.waitIdle();
  }
  public addTask(context: Context, execute: PoolTask): void {
    this.#core.addTask(context, 0, execute);
  }
}

export class WorkflowPriorityTaskPool implements PriorityTaskPoolLike {
  readonly #core: WorkflowPoolCore;

  public constructor(
    name: string,
    executors: number,
    metrics: Metrics,
    service: string,
    onError: (error: unknown) => void
  ) {
    this.#core = new WorkflowPoolCore(name, executors, true, metrics, service, onError);
  }

  public name(): string {
    return this.#core.name();
  }
  public executorsCount(): number {
    return this.#core.executorsCount();
  }
  public queueLength(): number {
    return this.#core.queueLength();
  }
  public activeCount(): number {
    return this.#core.activeCount();
  }
  public start(): void {
    this.#core.start();
  }
  public stop(): Promise<void> {
    return this.#core.stop();
  }
  public waitIdle(): Promise<void> {
    return this.#core.waitIdle();
  }
  public addTask(context: Context, priority: number, execute: PoolTask): void {
    this.#core.addTask(context, priority, execute);
  }
}
