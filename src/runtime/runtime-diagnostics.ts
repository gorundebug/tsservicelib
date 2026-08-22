import {
  monitorEventLoopDelay,
  PerformanceObserver,
  performance,
  type IntervalHistogram,
  type PerformanceEntry
} from "node:perf_hooks";

import { Context } from "./context.js";
import type { Metrics } from "./environment/index.js";
import type { Lifecycle } from "./lifecycle.js";
import type { PriorityTaskPool, TaskPool } from "./pool/index.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";

const NANOSECONDS_PER_SECOND = 1_000_000_000;

/**
 * Node-specific runtime diagnostics. Sampling exists only when metrics are
 * enabled, so benchmark builds using NoopMetricsEngine keep the zero-work
 * telemetry path.
 */
export class RuntimeDiagnostics implements Lifecycle {
  readonly #eventLoopDelay: IntervalHistogram | undefined;
  readonly #gcObserver: PerformanceObserver | undefined;
  #eventLoopUtilization = 0;
  #previousUtilization = performance.eventLoopUtilization();
  #started = false;

  public constructor(
    metrics: Metrics,
    service: string,
    tasks: RuntimeTaskRegistry,
    taskPools: readonly TaskPool[],
    priorityTaskPools: readonly PriorityTaskPool[]
  ) {
    if (!metrics.enabled()) return;

    this.#eventLoopDelay = monitorEventLoopDelay({ resolution: 10 });
    const scope = metrics.scope("runtime", { service });
    scope.observableFloat64Gauge(
      "event_loop_lag_seconds",
      "Maximum Node.js event-loop delay since the previous metrics scrape",
      () => this.eventLoopLagSeconds()
    );
    scope.observableFloat64Gauge(
      "worker_utilization",
      "Fraction of the single Node.js event-loop worker used since the previous metrics scrape",
      () => this.sampleEventLoopUtilization()
    );
    scope.observableFloat64Gauge(
      "worker_count",
      "Number of JavaScript event-loop workers in this service process",
      () => 1
    );
    scope.observableFloat64Gauge(
      "active_work",
      "Accepted runtime tasks and pool executors currently active",
      () =>
        tasks.activeCount() +
        taskPools.reduce((total, pool) => total + pool.activeCount(), 0) +
        priorityTaskPools.reduce((total, pool) => total + pool.activeCount(), 0)
    );
    scope.observableFloat64Gauge(
      "active_resources",
      "Active Node.js resources that keep the event loop alive",
      () => process.getActiveResourcesInfo().length
    );
    scope.observableFloat64Gauge(
      "task_queue_length",
      "Runtime task-pool entries waiting for execution",
      () =>
        taskPools.reduce((total, pool) => total + pool.queueLength(), 0) +
        priorityTaskPools.reduce((total, pool) => total + pool.queueLength(), 0)
    );

    const pauses = scope.histogram(
      "gc_pause_seconds",
      "Observed V8 garbage-collection pause duration",
      {},
      [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
    );
    const collections = scope.counterVec(
      "gc_collections_total",
      "Observed V8 garbage-collection events"
    );
    this.#gcObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        pauses.observe(Context.background(), entry.duration / 1_000);
        collections.with({ kind: gcKind(entry) }).inc(Context.background());
      }
    });
  }

  public start(context: Context): Promise<void> {
    void context;
    if (this.#started) return Promise.reject(new Error("runtime diagnostics already started"));
    this.#started = true;
    this.#previousUtilization = performance.eventLoopUtilization();
    this.#eventLoopDelay?.enable();
    this.#gcObserver?.observe({ entryTypes: ["gc"] });
    return Promise.resolve();
  }

  public stop(context: Context): Promise<void> {
    void context;
    if (!this.#started) return Promise.resolve();
    this.#started = false;
    this.#eventLoopDelay?.disable();
    this.#gcObserver?.disconnect();
    return Promise.resolve();
  }

  private eventLoopLagSeconds(): number {
    const delay = this.#eventLoopDelay;
    if (delay === undefined || !this.#started || !Number.isFinite(delay.max)) return 0;
    const maximum = delay.max / NANOSECONDS_PER_SECOND;
    delay.reset();
    return maximum;
  }

  private sampleEventLoopUtilization(): number {
    if (!this.#started) return this.#eventLoopUtilization;
    const current = performance.eventLoopUtilization();
    this.#eventLoopUtilization = performance.eventLoopUtilization(
      current,
      this.#previousUtilization
    ).utilization;
    this.#previousUtilization = current;
    return this.#eventLoopUtilization;
  }
}

function gcKind(entry: PerformanceEntry): string {
  const detail = (entry as PerformanceEntry & { readonly detail?: { readonly kind?: unknown } })
    .detail;
  return typeof detail?.kind === "number" ? String(detail.kind) : "unknown";
}
