import { Context } from "./context.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import { err, str } from "./environment/index.js";
import { RuntimeStoppedError } from "./errors.js";
import type { AdmissionLifecycle, ComponentCategory, RuntimeComponent } from "./lifecycle.js";
import { RuntimeTaskRegistry } from "./task-registry.js";

const START_ORDER: readonly ComponentCategory[] = [
  "dataSource",
  "dataSink",
  "managedDataConnector",
  "storage",
  "delayPool",
  "taskPool",
  "priorityTaskPool",
  "component",
  "httpServer",
  "telemetry"
];

const ADMISSION_CATEGORIES: ReadonlySet<ComponentCategory> = new Set([
  "dataSource",
  "managedDataConnector",
  "component",
  "httpServer"
]);

export class ServiceRuntime {
  readonly #components: RuntimeComponent[] = [];
  readonly #environment: RuntimeEnvironment;
  readonly #startupController = new AbortController();
  readonly #started: RuntimeComponent[] = [];
  readonly #tasks: RuntimeTaskRegistry;
  #startPromise: Promise<void> | undefined;
  #state: "created" | "starting" | "running" | "stopping" | "stopped" = "created";
  #stopPromise: Promise<void> | undefined;

  public constructor(environment: RuntimeEnvironment, tasks = new RuntimeTaskRegistry()) {
    this.#environment = environment;
    this.#tasks = tasks;
  }

  public tasks(): RuntimeTaskRegistry {
    return this.#tasks;
  }

  public state(): "created" | "starting" | "running" | "stopping" | "stopped" {
    return this.#state;
  }

  public register(component: RuntimeComponent): void {
    if (this.#state !== "created") {
      throw new Error("runtime components must be registered before start");
    }
    if (
      this.#components.some(
        ({ category, name }) => category === component.category && name === component.name
      )
    ) {
      throw new Error(`duplicate runtime component ${component.category}:${component.name}`);
    }
    this.#components.push(component);
  }

  public start(context = Context.background()): Promise<void> {
    if (this.#state !== "created") {
      return Promise.reject(new Error(`runtime cannot start from state ${this.#state}`));
    }
    this.#state = "starting";
    const startupContext = context.withExternalCancellation(this.#startupController.signal);
    this.#startPromise = this.startOnce(startupContext);
    return this.#startPromise;
  }

  private async startOnce(context: Context): Promise<void> {
    try {
      await this.#environment.buildRuntimeStreams();
      context.signal().throwIfAborted();
      this.#environment.validateRuntimeTopology();
      for (const category of START_ORDER) {
        for (const component of this.#components.filter((item) => item.category === category)) {
          context.signal().throwIfAborted();
          await component.lifecycle.start(context);
          this.#started.push(component);
          context.signal().throwIfAborted();
        }
      }
      this.#state = "running";
    } catch (error: unknown) {
      this.#tasks.cancel(error);
      await this.rollback(context.withoutCancellation());
      this.#state = "stopped";
      throw error;
    }
  }

  public stop(context = Context.background(), drainTimeoutMs?: number): Promise<void> {
    if (this.#stopPromise !== undefined) {
      return this.#stopPromise;
    }
    this.#stopPromise = this.stopOnce(context, drainTimeoutMs);
    return this.#stopPromise;
  }

  private async stopOnce(context: Context, drainTimeoutMs: number | undefined): Promise<void> {
    const stopContext =
      drainTimeoutMs === undefined ? context : context.bounded(Math.max(0, drainTimeoutMs));
    if (this.#state === "starting") {
      this.#startupController.abort(new RuntimeStoppedError("runtime startup was stopped"));
      try {
        await this.#startPromise;
      } catch {
        // startOnce owns partial-start rollback and preserves its error for the start caller.
      }
    }
    if (this.#state === "created" || this.#state === "stopped") {
      this.#tasks.stopAdmission();
      this.#state = "stopped";
      return;
    }
    this.#state = "stopping";

    const admission = this.#started.filter((item) => ADMISSION_CATEGORIES.has(item.category));
    await this.stopAdmission(admission, stopContext);
    try {
      await this.#tasks.drain(stopContext.remainingMs());
      this.#tasks.stopAdmission();
    } catch (error: unknown) {
      this.#tasks.cancel(error);
      throw error;
    } finally {
      await this.stopConcurrent(
        this.#started.filter(
          (item) =>
            (item.category === "dataSource" && "stopAdmission" in item.lifecycle) ||
            item.category === "dataSink" ||
            item.category === "managedDataConnector" ||
            item.category === "storage" ||
            item.category === "delayPool" ||
            item.category === "taskPool" ||
            item.category === "priorityTaskPool"
        ),
        stopContext
      );
      await this.stopSequential(
        this.#started.filter((item) => item.category === "telemetry"),
        stopContext
      );
      this.#started.length = 0;
      this.#state = "stopped";
    }
  }

  private async rollback(context: Context): Promise<void> {
    for (const component of this.#started.toReversed()) {
      try {
        await component.lifecycle.stop(context);
      } catch (error: unknown) {
        // Preserve the original startup failure while still attempting every rollback.
        this.logStopError(context, component, error);
      }
    }
    this.#started.length = 0;
  }

  private async stopConcurrent(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    const ordered = components.toReversed();
    const results = await settleWithinDeadline(
      context,
      ordered.map(async (item) => item.lifecycle.stop(context))
    );
    for (const [index, result] of results.entries()) {
      const component = ordered[index];
      if (component === undefined) continue;
      if (result === undefined) {
        this.logStopTimeout(context, component);
        continue;
      }
      if (result.status === "rejected") {
        this.logStopError(context, component, result.reason);
      }
    }
  }

  private async stopAdmission(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    const ordered = components.toReversed();
    const results = await settleWithinDeadline(
      context,
      ordered.map(async (item) => {
        if ("stopAdmission" in item.lifecycle) {
          await (item.lifecycle as AdmissionLifecycle).stopAdmission(context);
          return;
        }
        await item.lifecycle.stop(context);
      })
    );
    for (const [index, result] of results.entries()) {
      const component = ordered[index];
      if (component === undefined) continue;
      if (result === undefined) {
        this.logStopTimeout(context, component);
        continue;
      }
      if (result.status === "rejected") {
        this.logStopError(context, component, result.reason);
      }
    }
  }

  private async stopSequential(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    for (const component of components.toReversed()) {
      const [result] = await settleWithinDeadline(context, [component.lifecycle.stop(context)]);
      if (result === undefined) {
        this.logStopTimeout(context, component);
      } else if (result.status === "rejected") {
        this.logStopError(context, component, result.reason);
      }
    }
  }

  private logStopTimeout(context: Context, component: RuntimeComponent): void {
    this.#environment
      .log()
      .warn(
        context,
        "runtime component shutdown timed out",
        str("category", component.category),
        str("component", component.name)
      );
  }

  private logStopError(context: Context, component: RuntimeComponent, error: unknown): void {
    this.#environment
      .log()
      .warn(
        context,
        "runtime component shutdown",
        str("category", component.category),
        str("component", component.name),
        err(error instanceof Error ? error : new Error(String(error)))
      );
  }
}

async function settleWithinDeadline<T>(
  context: Context,
  operations: readonly Promise<T>[]
): Promise<readonly (PromiseSettledResult<T> | undefined)[]> {
  if (operations.length === 0) return [];
  const results = new Array<PromiseSettledResult<T> | undefined>(operations.length);
  const tracked = operations.map(async (operation, index) => {
    try {
      results[index] = { status: "fulfilled", value: await operation };
    } catch (reason: unknown) {
      results[index] = { status: "rejected", reason };
    }
  });
  const remainingMs = context.remainingMs();
  if (remainingMs === undefined) {
    await Promise.all(tracked);
    return results;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, Math.max(0, remainingMs));
  });
  try {
    await Promise.race([Promise.all(tracked), timeout]);
    return results;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
