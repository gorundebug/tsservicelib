import { performance } from "node:perf_hooks";

import type { Context } from "../context.js";
import {
  err,
  type Float64Histogram,
  type Int64Counter,
  type Int64Gauge,
  type Logger,
  type Metrics
} from "../environment/index.js";
import type { Lifecycle } from "../lifecycle.js";
import type { Completion } from "../stream.js";
import { PoolStoppedError } from "./pool.js";

export interface DelayPoolOptions {
  readonly name?: string;
  readonly onError?: (error: unknown) => void;
  readonly logger?: Logger;
  readonly metrics?: Metrics;
  readonly service?: string;
}

interface DelayTask {
  completed: boolean;
  timer: NodeJS.Timeout | undefined;
  removeAbortListener(): void;
}

interface DelayPoolMetrics {
  readonly waitQueueLength: Int64Gauge;
  readonly tasksTotal: Int64Counter;
  readonly executionDuration: Float64Histogram;
  readonly stopTimeout: Int64Counter;
  readonly taskCancelled: Int64Counter;
}

export class DelayPool implements Lifecycle {
  readonly #name: string;
  readonly #onError: (error: unknown) => void;
  readonly #logger: Logger | undefined;
  readonly #metrics: DelayPoolMetrics | undefined;
  readonly #tasks = new Set<DelayTask>();
  #state: "created" | "running" | "stopping" | "stopped" = "created";
  #drain: Promise<void> | undefined;
  #resolveDrain: (() => void) | undefined;

  public constructor(options: DelayPoolOptions = {}) {
    this.#name = options.name ?? "delay";
    this.#onError = options.onError ?? (() => undefined);
    this.#logger = options.logger;
    this.#metrics = makeMetrics(options.metrics, options.service);
  }

  public pendingCount(): number {
    return this.#tasks.size;
  }

  public start(context: Context): Promise<void> {
    void context;
    if (this.#state !== "created") {
      return Promise.reject(new Error(`pool ${this.#name} cannot start from ${this.#state}`));
    }
    this.#state = "running";
    return Promise.resolve();
  }

  public delay(context: Context, delayMs: number, execute: () => Completion): void {
    if (context.cancelled()) {
      throw context.signal().reason ?? new Error("delay context is cancelled");
    }
    if (this.#state === "stopping" || this.#state === "stopped") {
      throw new PoolStoppedError(this.#name);
    }

    const remaining = context.remainingMs();
    const effectiveDelay = Math.max(0, Math.min(delayMs, remaining ?? Infinity));
    const task: DelayTask = {
      completed: false,
      timer: undefined,
      removeAbortListener: () => undefined
    };
    this.#metrics?.waitQueueLength.inc();
    const finish = (cancelled: boolean): void => {
      if (task.completed) {
        return;
      }
      task.completed = true;
      if (task.timer !== undefined) {
        clearTimeout(task.timer);
      }
      task.removeAbortListener();
      const started = performance.now();
      let completion: Completion;
      try {
        completion = execute();
      } catch (error: unknown) {
        this.#onError(error);
        this.completeTask(task, context, started, cancelled);
        return;
      }
      void Promise.resolve(completion)
        .catch((error: unknown) => {
          this.#onError(error);
        })
        .finally(() => {
          this.completeTask(task, context, started, cancelled);
        });
    };
    const cancelled = (): void => {
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
    } else {
      task.timer = setTimeout(() => {
        finish(false);
      }, effectiveDelay);
    }
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
    let removeAbortListener = (): void => undefined;
    const aborted = new Promise<"aborted">((resolve) => {
      const listener = (): void => {
        resolve("aborted");
      };
      context.signal().addEventListener("abort", listener, { once: true });
      removeAbortListener = () => {
        context.signal().removeEventListener("abort", listener);
      };
    });
    try {
      if ((await Promise.race([drain.then(() => "drained" as const), aborted])) === "aborted") {
        this.reportStopTimeout(context);
        await drain;
      }
    } finally {
      removeAbortListener();
    }
  }

  private completeTask(
    task: DelayTask,
    context: Context,
    started: number,
    cancelled: boolean
  ): void {
    this.#tasks.delete(task);
    this.#metrics?.waitQueueLength.dec();
    this.#metrics?.tasksTotal.inc(context);
    this.#metrics?.executionDuration.observe(context, (performance.now() - started) / 1_000);
    if (cancelled) this.#metrics?.taskCancelled.inc(context);
    this.finishDrainIfIdle();
  }

  private reportStopTimeout(context: Context): void {
    const reason = context.signal().reason as unknown;
    const error = reason instanceof Error ? reason : new Error("delay pool stop timed out");
    this.#logger?.warn(context, "delay pool stopped by timeout", err(error));
    this.#metrics?.stopTimeout.inc(context);
  }

  private finishDrainIfIdle(): void {
    if (this.#state !== "stopping" || this.#tasks.size !== 0) {
      return;
    }
    this.#state = "stopped";
    this.#resolveDrain?.();
    this.#resolveDrain = undefined;
  }
}

function makeMetrics(
  metrics: Metrics | undefined,
  service: string | undefined
): DelayPoolMetrics | undefined {
  if (metrics?.enabled() !== true || service === undefined) return undefined;
  const scope = metrics.scope("delay_pool", { service });
  const waitQueueLength = scope.gauge("wait_queue_length", "Delay pool wait queue length");
  waitQueueLength.set(0);
  const events = scope.counterVec("events_total", "Total number of events in delay pool");
  return {
    waitQueueLength,
    tasksTotal: scope.counter("tasks_total", "Total number of tasks executed by delay pool"),
    executionDuration: scope.histogram(
      "task_execution_duration_seconds",
      "Task execution duration in seconds"
    ),
    stopTimeout: events.with({ event: "stop_timeout" }),
    taskCancelled: events.with({ event: "task_cancelled" })
  };
}
