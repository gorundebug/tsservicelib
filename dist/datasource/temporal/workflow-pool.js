import { PoolStoppedError } from "../../runtime/pool/pool.js";
import { makeTaskPoolMetrics } from "../../runtime/pool/task-pool-metrics.js";
class WorkflowPoolCore {
  #name;
  #executors;
  #priority;
  #onError;
  #queue = [];
  #metrics;
  #active = 0;
  #sequence = 0;
  #state = "created";
  #resolveDrain;
  #drain;
  constructor(name, executors, priority, metrics, service, onError) {
    if (!Number.isInteger(executors) || executors < 1) {
      throw new RangeError("executors must be a positive integer");
    }
    this.#name = name;
    this.#executors = executors;
    this.#priority = priority;
    this.#onError = onError;
    this.#metrics = makeTaskPoolMetrics(priority ? "priority" : "task", {
      name,
      executorsCount: executors,
      metrics,
      service
    });
  }
  name() {
    return this.#name;
  }
  executorsCount() {
    return this.#executors;
  }
  queueLength() {
    return this.#queue.length;
  }
  activeCount() {
    return this.#active;
  }
  start() {
    if (this.#state !== "created") {
      throw new Error(`workflow pool ${this.#name} cannot start from ${this.#state}`);
    }
    this.#state = "running";
    this.#metrics?.executorsTarget.set(this.#executors);
    this.#metrics?.executorsAllocated.set(this.#executors);
    this.pump();
  }
  addTask(context, priority, execute) {
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
      execute,
      priority,
      sequence: this.#sequence,
      removeAbortListener: () => undefined
    };
    this.#sequence += 1;
    const cancel = () => {
      const index = this.#queue.indexOf(task);
      if (this.#priority ? index >= 0 : index > 0) {
        this.#queue.splice(index, 1);
        if (this.#priority) {
          task.priority = Number.NEGATIVE_INFINITY;
          this.insert(task);
        } else {
          this.#queue.unshift(task);
        }
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
  async stop() {
    if (this.#state === "stopped") return;
    if (this.#drain !== undefined) return this.#drain;
    this.#state = "stopping";
    this.#drain = new Promise((resolve) => {
      this.#resolveDrain = resolve;
    });
    this.pump();
    this.finishDrainIfIdle();
    await this.#drain;
  }
  pump() {
    if (this.#state !== "running" && this.#state !== "stopping") return;
    while (this.#active < this.#executors && this.#queue.length > 0) {
      const task = this.#queue.shift();
      if (task === undefined) break;
      task.removeAbortListener();
      this.#metrics?.queueLength.dec();
      this.#active += 1;
      void Promise.resolve().then(() => this.run(task));
    }
    this.finishDrainIfIdle();
  }
  async run(task) {
    const started = Date.now();
    this.#metrics?.executorsBusy.inc();
    try {
      await task.execute();
    } catch (error) {
      this.#onError(error);
    } finally {
      this.#metrics?.executorsBusy.dec();
      this.#metrics?.tasksTotal.inc(task.context);
      this.#metrics?.executionDuration.observe(task.context, (Date.now() - started) / 1_000);
      this.#active -= 1;
      this.pump();
    }
  }
  insert(task) {
    if (!this.#priority) {
      this.#queue.push(task);
      return;
    }
    const index = this.#queue.findIndex(
      (item) =>
        item.priority > task.priority ||
        (item.priority === task.priority && item.sequence > task.sequence)
    );
    if (index < 0) this.#queue.push(task);
    else this.#queue.splice(index, 0, task);
  }
  finishDrainIfIdle() {
    if (this.#state !== "stopping" || this.#active !== 0 || this.#queue.length !== 0) return;
    this.#state = "stopped";
    this.#metrics?.queueLength.set(0);
    this.#metrics?.executorsBusy.set(0);
    this.#metrics?.executorsAllocated.set(0);
    this.#resolveDrain?.();
    this.#resolveDrain = undefined;
  }
}
export class WorkflowTaskPool {
  #core;
  constructor(name, executors, metrics, service, onError) {
    this.#core = new WorkflowPoolCore(name, executors, false, metrics, service, onError);
  }
  name() {
    return this.#core.name();
  }
  executorsCount() {
    return this.#core.executorsCount();
  }
  queueLength() {
    return this.#core.queueLength();
  }
  activeCount() {
    return this.#core.activeCount();
  }
  start() {
    this.#core.start();
  }
  stop() {
    return this.#core.stop();
  }
  addTask(context, execute) {
    this.#core.addTask(context, 0, execute);
  }
}
export class WorkflowPriorityTaskPool {
  #core;
  constructor(name, executors, metrics, service, onError) {
    this.#core = new WorkflowPoolCore(name, executors, true, metrics, service, onError);
  }
  name() {
    return this.#core.name();
  }
  executorsCount() {
    return this.#core.executorsCount();
  }
  queueLength() {
    return this.#core.queueLength();
  }
  activeCount() {
    return this.#core.activeCount();
  }
  start() {
    this.#core.start();
  }
  stop() {
    return this.#core.stop();
  }
  addTask(context, priority, execute) {
    this.#core.addTask(context, priority, execute);
  }
}
//# sourceMappingURL=workflow-pool.js.map
