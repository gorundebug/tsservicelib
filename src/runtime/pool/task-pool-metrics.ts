import type { Context } from "../context.js";
import type { Float64Histogram, Int64Counter, Int64Gauge, Metrics } from "../environment/index.js";

import type { TaskPoolOptions } from "./pool.js";

export interface TaskPoolMetrics {
  readonly queueLength: Int64Gauge;
  readonly executorsTarget: Int64Gauge;
  readonly executorsAllocated: Int64Gauge;
  readonly executorsBusy: Int64Gauge;
  readonly tasksTotal: Int64Counter;
  readonly executionDuration: Float64Histogram;
  readonly stopTimeout: Int64Counter;
  readonly taskRejected: Int64Counter;
  readonly taskCancelledOrExpired: Int64Counter;
}

export function makeTaskPoolMetrics(
  kind: "task" | "priority",
  options: TaskPoolOptions
): TaskPoolMetrics | undefined {
  if (options.metrics?.enabled() !== true || options.service === undefined) return undefined;
  return registerTaskPoolMetrics(kind, options.metrics, options.service, options.name);
}

function registerTaskPoolMetrics(
  kind: "task" | "priority",
  metrics: Metrics,
  service: string,
  name: string
): TaskPoolMetrics {
  const priority = kind === "priority";
  const scope = metrics.scope(priority ? "priority_task_pool" : "task_pool", { service, name });
  const queueLength = scope.gauge(
    "queue_length",
    priority ? "Priority task pool wait queue length" : "Task pool wait queue length"
  );
  const executorsTarget = scope.gauge(
    "executors_target",
    priority
      ? "Desired number of priority task pool executors"
      : "Desired number of task pool executors"
  );
  const executorsAllocated = scope.gauge(
    "executors_allocated",
    priority ? "Number of live priority task pool executors" : "Number of live task pool executors"
  );
  const executorsBusy = scope.gauge(
    "executors_busy",
    priority
      ? "Number of priority task pool executors running callbacks"
      : "Number of task pool executors running callbacks"
  );
  for (const gauge of [queueLength, executorsTarget, executorsAllocated, executorsBusy]) {
    gauge.set(0);
  }
  const events = scope.counterVec(
    "events_total",
    priority
      ? "Total number of events in priority task pool"
      : "Total number of events in task pool"
  );
  return {
    queueLength,
    executorsTarget,
    executorsAllocated,
    executorsBusy,
    tasksTotal: scope.counter(
      "tasks_total",
      priority
        ? "Total number of tasks executed by priority task pool"
        : "Total number of tasks executed by task pool"
    ),
    executionDuration: scope.histogram(
      "task_execution_duration_seconds",
      "Task execution duration in seconds"
    ),
    stopTimeout: events.with({ event: "stop_timeout" }),
    taskRejected: events.with({ event: "task_rejected" }),
    taskCancelledOrExpired: events.with({
      event: priority ? "task_expired" : "task_cancelled"
    })
  };
}

export async function awaitPoolDrain(
  drain: Promise<void>,
  context: Context,
  onTimeout: () => void
): Promise<void> {
  if (context.cancelled()) {
    onTimeout();
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
      onTimeout();
      await drain;
    }
  } finally {
    removeAbortListener();
  }
}
