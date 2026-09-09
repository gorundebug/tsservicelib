import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { IndexedHeap } from "./indexed-heap.js";
import { subscribeAbort, reportPoolError } from "./pool-support.js";
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
  readonly runAt: number;
  readonly sequence: number;
  readonly context: Context;
  readonly execute: () => Completion;
  readonly expeditedByDeadline: boolean;
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
  readonly #schedule = AsyncLocalStorage.snapshot();
  readonly #name: string;
  readonly #onError: (error: unknown) => void;
  readonly #logger: Logger | undefined;
  readonly #metrics: DelayPoolMetrics | undefined;
  readonly #tasks = new Set<DelayTask>();
  readonly #queue = new IndexedHeap<DelayTask>(
    (a, b) => a.runAt - b.runAt || a.sequence - b.sequence
  );
  #timer: NodeJS.Timeout | undefined;
  #armedAt: number | undefined;
  #sequence = 0;
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

    if (Number.isNaN(delayMs)) throw new RangeError("delay must not be NaN");
    const remaining = context.remainingMs();
    const effectiveDelay = Math.max(0, Math.min(delayMs, remaining ?? Infinity));
    const task: DelayTask = {
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

  private arm(): void {
    const next = this.#queue.peek()?.runAt;
    if (next === this.#armedAt) return;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#armedAt = next;
    if (next === undefined || next === Infinity) return;
    // Node otherwise turns delays above INT32_MAX into a one millisecond timer.
    this.#timer = this.#schedule(() =>
      setTimeout(
        () => {
          this.#timer = undefined;
          this.#armedAt = undefined;
          const now = performance.now();
          for (;;) {
            const task = this.#queue.peek();
            if (task === undefined || task.runAt > now) break;
            this.dispatch(task, task.expeditedByDeadline || task.context.cancelled());
          }
          this.arm();
        },
        Math.min(2_147_483_647, Math.max(0, Math.ceil(next - performance.now())))
      )
    );
  }

  private dispatch(task: DelayTask, cancelled: boolean): void {
    if (task.completed) return;
    task.completed = true;
    this.#queue.remove(task);
    task.removeAbortListener();
    // In particular, abort listeners must never invoke user code inline.
    queueMicrotask(() => {
      const started = performance.now();
      let completion: Completion;
      try {
        completion = task.execute();
      } catch (error: unknown) {
        reportPoolError(this.#onError, error);
        this.completeTask(task, task.context, started, cancelled);
        return;
      }
      void Promise.resolve(completion)
        .catch((error: unknown) => {
          reportPoolError(this.#onError, error);
        })
        .finally(() => {
          this.completeTask(task, task.context, started, cancelled);
        });
    });
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
