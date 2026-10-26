import { ContextDeadlineExceededError, RuntimeDrainTimeoutError, RuntimeStoppedError, errorFromUnknown } from "../errors.js";
import { settleWithinDeadline } from "../lifecycle.js";
export function makeTaskPoolMetrics(kind, options) {
    if (options.metrics?.enabled() !== true || options.service === undefined)
        return undefined;
    return registerTaskPoolMetrics(kind, options.metrics, options.service, options.name);
}
function registerTaskPoolMetrics(kind, metrics, service, name) {
    const priority = kind === "priority";
    const scope = metrics.scope(priority ? "priority_task_pool" : "task_pool", { service, name });
    const queueLength = scope.gauge("queue_length", priority ? "Priority task pool wait queue length" : "Task pool wait queue length");
    const executorsTarget = scope.gauge("executors_target", priority
        ? "Desired number of priority task pool executors"
        : "Desired number of task pool executors");
    const executorsAllocated = scope.gauge("executors_allocated", priority ? "Number of live priority task pool executors" : "Number of live task pool executors");
    const executorsBusy = scope.gauge("executors_busy", priority
        ? "Number of priority task pool executors running callbacks"
        : "Number of task pool executors running callbacks");
    for (const gauge of [queueLength, executorsTarget, executorsAllocated, executorsBusy]) {
        gauge.set(0);
    }
    const events = scope.counterVec("events_total", priority
        ? "Total number of events in priority task pool"
        : "Total number of events in task pool");
    return {
        queueLength,
        executorsTarget,
        executorsAllocated,
        executorsBusy,
        tasksTotal: scope.counter("tasks_total", priority
            ? "Total number of tasks executed by priority task pool"
            : "Total number of tasks executed by task pool"),
        executionDuration: scope.histogram("task_execution_duration_seconds", "Task execution duration in seconds"),
        stopTimeout: events.with({ event: "stop_timeout" }),
        taskRejected: events.with({ event: "task_rejected" }),
        taskCancelledOrExpired: events.with({
            event: priority ? "task_expired" : "task_cancelled"
        })
    };
}
/** Bound this caller's wait without cancelling or discarding the physical drain. */
export async function awaitPoolDrain(drain, context, onTimeout) {
    const budget = context.remainingMs();
    const [result] = await settleWithinDeadline(context, [drain]);
    if (result?.status === "fulfilled")
        return;
    if (result?.status === "rejected")
        throw errorFromUnknown(result.reason);
    onTimeout();
    const reason = context.signal().reason;
    if (reason instanceof ContextDeadlineExceededError ||
        (!context.cancelled() && context.remainingMs() === 0)) {
        throw new RuntimeDrainTimeoutError(budget ?? 0);
    }
    throw errorFromUnknown(reason ?? new RuntimeStoppedError("pool shutdown was cancelled"));
}
//# sourceMappingURL=task-pool-metrics.js.map