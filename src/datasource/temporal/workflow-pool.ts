import type { Context } from "../../runtime/context.js";
import type { Metrics } from "../../runtime/environment/metrics/metrics.js";
import {
  PoolStoppedError,
  type PoolTask,
  type PriorityTaskPoolLike,
  type TaskPoolLike
} from "../../runtime/pool/pool.js";

interface WorkflowTask {
  readonly context: Context;
  readonly execute: PoolTask;
  readonly priority: number;
  readonly sequence: number;
}

interface WorkflowPoolMetrics {
  readonly queued: Gauge;
  readonly active: Gauge;
  readonly completed: Counter;
}

interface Gauge {
  inc(): void;
  dec(): void;
  set(value: number): void;
}

interface Counter {
  inc(context: Context): void;
}

class WorkflowPoolCore {
  readonly #name: string;
  readonly #executors: number;
  readonly #priority: boolean;
  readonly #onError: (error: unknown) => void;
  readonly #queue: WorkflowTask[] = [];
  readonly #metrics: WorkflowPoolMetrics;
  #active = 0;
  #sequence = 0;
  #state: "created" | "running" | "stopping" | "stopped" = "created";
  #resolveDrain: (() => void) | undefined;
  #drain: Promise<void> | undefined;

  public constructor(
    name: string,
    executors: number,
    priority: boolean,
    metrics: Metrics,
    service: string,
    onError: (error: unknown) => void
  ) {
    if (!Number.isInteger(executors) || executors < 1) {
      throw new RangeError("executors must be a positive integer");
    }
    this.#name = name;
    this.#executors = executors;
    this.#priority = priority;
    this.#onError = onError;
    const scope = metrics.scope("taskpool", {
      service,
      taskpoolname: name,
      type: priority ? "priority" : "task"
    });
    this.#metrics = {
      queued: scope.gauge("queue_length", "Current number of queued tasks"),
      active: scope.gauge("executors_busy", "Current number of executing tasks"),
      completed: scope.counter("tasks_total", "Total number of completed tasks")
    };
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
    this.pump();
  }

  public addTask(context: Context, priority: number, execute: PoolTask): void {
    if (context.cancelled()) {
      throw context.signal().reason ?? new Error("task context is cancelled");
    }
    if (this.#state === "stopping" || this.#state === "stopped") {
      throw new PoolStoppedError(this.#name);
    }
    const task = { context, execute, priority, sequence: this.#sequence };
    this.#sequence += 1;
    if (this.#priority) {
      const index = this.#queue.findIndex(
        (item) =>
          item.priority > task.priority ||
          (item.priority === task.priority && item.sequence > task.sequence)
      );
      if (index < 0) this.#queue.push(task);
      else this.#queue.splice(index, 0, task);
    } else {
      this.#queue.push(task);
    }
    this.#metrics.queued.inc();
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

  private pump(): void {
    if (this.#state !== "running" && this.#state !== "stopping") return;
    while (this.#active < this.#executors && this.#queue.length > 0) {
      const task = this.#queue.shift();
      if (task === undefined) break;
      this.#metrics.queued.dec();
      this.#metrics.active.inc();
      this.#active += 1;
      void Promise.resolve().then(() => this.run(task));
    }
    this.finishDrainIfIdle();
  }

  private async run(task: WorkflowTask): Promise<void> {
    try {
      await task.execute();
    } catch (error: unknown) {
      this.#onError(error);
    } finally {
      this.#metrics.active.dec();
      this.#metrics.completed.inc(task.context);
      this.#active -= 1;
      this.pump();
    }
  }

  private finishDrainIfIdle(): void {
    if (this.#state !== "stopping" || this.#active !== 0 || this.#queue.length !== 0) return;
    this.#state = "stopped";
    this.#metrics.queued.set(0);
    this.#metrics.active.set(0);
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
  public addTask(context: Context, priority: number, execute: PoolTask): void {
    this.#core.addTask(context, priority, execute);
  }
}
