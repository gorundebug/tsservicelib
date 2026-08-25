import { PoolStoppedError } from "../../runtime/pool/pool.js";
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
    this.pump();
  }
  addTask(context, priority, execute) {
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
      this.#metrics.queued.dec();
      this.#metrics.active.inc();
      this.#active += 1;
      void Promise.resolve().then(() => this.run(task));
    }
    this.finishDrainIfIdle();
  }
  async run(task) {
    try {
      await task.execute();
    } catch (error) {
      this.#onError(error);
    } finally {
      this.#metrics.active.dec();
      this.#metrics.completed.inc(task.context);
      this.#active -= 1;
      this.pump();
    }
  }
  finishDrainIfIdle() {
    if (this.#state !== "stopping" || this.#active !== 0 || this.#queue.length !== 0) return;
    this.#state = "stopped";
    this.#metrics.queued.set(0);
    this.#metrics.active.set(0);
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
