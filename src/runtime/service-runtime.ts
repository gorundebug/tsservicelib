import { Context } from "./context.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import { err, str } from "./environment/index.js";
import { RuntimeStoppedError } from "./errors.js";
import type { ComponentCategory, RuntimeComponent } from "./lifecycle.js";
import { RuntimeTaskRegistry } from "./task-registry.js";

const START_ORDER: readonly ComponentCategory[] = [
  "dataSource",
  "dataSink",
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
  "delayPool",
  "taskPool",
  "priorityTaskPool",
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
    await this.stopConcurrent(admission, context);
    try {
      await this.#tasks.drain(drainTimeoutMs);
      this.#tasks.stopAdmission();
    } catch (error: unknown) {
      this.#tasks.cancel(error);
      await this.#tasks.drain();
      throw error;
    } finally {
      await this.stopConcurrent(
        this.#started.filter((item) => item.category === "dataSink" || item.category === "storage"),
        context
      );
      await this.stopSequential(
        this.#started.filter((item) => item.category === "telemetry"),
        context
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
    const results = await Promise.allSettled(
      components.toReversed().map(async (item) => item.lifecycle.stop(context))
    );
    for (const [index, result] of results.entries()) {
      if (result.status === "rejected") {
        const component = components[components.length - index - 1];
        if (component !== undefined) this.logStopError(context, component, result.reason);
      }
    }
  }

  private async stopSequential(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    for (const component of components.toReversed()) {
      try {
        await component.lifecycle.stop(context);
      } catch (error: unknown) {
        this.logStopError(context, component, error);
      }
    }
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
