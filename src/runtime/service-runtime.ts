import type { Context } from "./context.js";
import type { ManagedDataConnector } from "./data-connector.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import { err, str } from "./environment/index.js";
import {
  ContextDeadlineExceededError,
  RuntimeDrainTimeoutError,
  RuntimeStoppedError,
  errorFromUnknown
} from "./errors.js";
import {
  settleWithinDeadline,
  type AdmissionLifecycle,
  type ComponentCategory,
  type RuntimeComponent
} from "./lifecycle.js";
import { RuntimeTaskRegistry } from "./task-registry.js";

const START_ORDER: readonly ComponentCategory[] = [
  "telemetry",
  "managedDataConnector",
  "storage",
  "delayPool",
  "taskPool",
  "priorityTaskPool",
  "component",
  "dataSink",
  "dataSource",
  "httpServer"
];

const ADMISSION_CATEGORIES: ReadonlySet<ComponentCategory> = new Set([
  "dataSource",
  "component",
  "httpServer"
]);

interface ComponentStop {
  readonly context: Context;
  readonly promise: Promise<void>;
}

export class ServiceRuntime {
  readonly #components: RuntimeComponent[] = [];
  readonly #environment: RuntimeEnvironment;
  readonly #startupController = new AbortController();
  readonly #started: RuntimeComponent[] = [];
  readonly #tasks: RuntimeTaskRegistry;
  readonly #admissionStops = new Map<RuntimeComponent, ComponentStop>();
  readonly #componentStops = new Map<RuntimeComponent, ComponentStop>();
  #rollbackPending = false;
  #startupPending = false;
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

  public start(context: Context): Promise<void> {
    if (this.#state !== "created") {
      return Promise.reject(new Error(`runtime cannot start from state ${this.#state}`));
    }
    this.#state = "starting";
    this.#startupPending = true;
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
        if (category === "dataSource") {
          for (const component of this.#started.filter(
            (item) => item.category === "managedDataConnector"
          )) {
            await (component.lifecycle as ManagedDataConnector).startAdmission(context);
            context.signal().throwIfAborted();
          }
        }
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
      this.#rollbackPending = true;
      try {
        await this.rollback(context);
      } catch {
        // Keep the original start error; unfinished rollback stays retryable.
      }
      this.#state =
        this.#started.length === 0 && this.#tasks.activeCount() === 0 ? "stopped" : "stopping";
      throw error;
    } finally {
      this.#startupPending = false;
    }
  }

  public async stop(context: Context, drainTimeoutMs?: number): Promise<void> {
    const stopContext =
      drainTimeoutMs === undefined ? context : context.bounded(Math.max(0, drainTimeoutMs));
    const budget = stopContext.remainingMs();
    // There is nothing to wait for, even if this caller is already cancelled.
    if (!this.#startupPending && this.#started.length === 0 && this.#tasks.activeCount() === 0) {
      this.#tasks.stopAdmission();
      this.#state = "stopped";
      return;
    }
    for (;;) {
      const ownsAttempt = this.#stopPromise === undefined;
      if (ownsAttempt) {
        const attempt = this.stopOnce(stopContext);
        this.#stopPromise = attempt;
        const release = (): void => {
          if (this.#stopPromise === attempt) this.#stopPromise = undefined;
        };
        void attempt.then(release, release);
      }
      const attempt = this.#stopPromise;
      if (attempt === undefined) continue;
      const [result] = await settleWithinDeadline(stopContext, [attempt]);
      if (result === undefined) {
        // Publish the failed wait only after sealing new task admission. Running
        // operations retain their ownership and can still be awaited by a retry.
        this.#tasks.stopAdmission();
        throw this.shutdownError(stopContext, budget);
      }
      if (result.status === "fulfilled") return;
      if (stopContext.cancelled() || stopContext.remainingMs() === 0) {
        this.#tasks.stopAdmission();
        throw this.shutdownError(stopContext, budget);
      }
      if (ownsAttempt) {
        this.#tasks.stopAdmission();
        throw errorFromUnknown(result.reason);
      }
      // The preceding caller's context may have expired. This caller can retry
      // with its own context, without duplicating still-running component stops.
    }
  }

  private async stopOnce(context: Context): Promise<void> {
    this.#state = "stopping";
    if (this.#startupPending) {
      this.#startupController.abort(new RuntimeStoppedError("runtime startup was stopped"));
      const budget = context.remainingMs();
      const [startup] = await settleWithinDeadline(context, [
        this.#startPromise ?? Promise.resolve()
      ]);
      if (startup === undefined) {
        const error = this.shutdownError(context, budget);
        this.#tasks.cancel(error);
        this.#environment.log().warn(context, "runtime startup shutdown timed out");
        throw error;
      }
    }
    if (this.#rollbackPending) {
      await this.rollback(context);
      await this.drainTasks(context);
      this.#state = "stopped";
      return;
    }

    await this.stopAdmission(
      this.#started.filter((item) => ADMISSION_CATEGORIES.has(item.category)),
      context
    );
    // Drain ordinary graph producers while outbound managed clients are alive.
    await this.stopConcurrent(
      this.#started.filter(
        (item) => ADMISSION_CATEGORIES.has(item.category) && "stopAdmission" in item.lifecycle
      ),
      context
    );
    await this.stopAdmission(
      this.#started.filter((item) => item.category === "managedDataConnector"),
      context
    );
    await this.stopConcurrent(
      this.#started.filter(
        (item) =>
          item.category === "storage" ||
          item.category === "delayPool" ||
          item.category === "taskPool" ||
          item.category === "priorityTaskPool"
      ),
      context
    );
    await this.drainTasks(context);
    await this.stopConcurrent(
      this.#started.filter((item) => item.category === "dataSink"),
      context
    );
    await this.stopConcurrent(
      this.#started.filter((item) => item.category === "managedDataConnector"),
      context
    );
    await this.stopSequential(
      this.#started.filter((item) => item.category === "telemetry"),
      context
    );
    this.#state = "stopped";
  }

  private async drainTasks(context: Context): Promise<void> {
    try {
      if (this.#tasks.activeCount() > 0) {
        const budget = context.remainingMs();
        const [result] = await settleWithinDeadline(context, [this.#tasks.drain(budget)]);
        if (result === undefined) throw this.shutdownError(context, budget);
        if (result.status === "rejected") throw errorFromUnknown(result.reason);
      }
      this.#tasks.stopAdmission();
    } catch (error: unknown) {
      this.#tasks.cancel(error);
      throw error;
    }
  }

  private async rollback(context: Context): Promise<void> {
    await this.stopSequential([...this.#started], context);
    this.#rollbackPending = false;
  }

  private stopComponent(
    component: RuntimeComponent,
    context: Context,
    admission: boolean
  ): Promise<void> {
    const admissionOnly = admission && "stopAdmission" in component.lifecycle;
    const pending = admissionOnly ? this.#admissionStops : this.#componentStops;
    const existing = pending.get(component);
    if (existing !== undefined) {
      if (existing.context === context) return existing.promise;
      return existing.promise.catch((error: unknown) => {
        const reason: unknown = existing.context.signal().reason;
        const previousWaitExpired =
          existing.context.cancelled() || existing.context.remainingMs() === 0;
        const cancellationError =
          error instanceof RuntimeDrainTimeoutError ||
          error instanceof ContextDeadlineExceededError ||
          (reason !== undefined && error === reason) ||
          (error instanceof Error && reason !== undefined && error.cause === reason);
        if (
          previousWaitExpired &&
          cancellationError &&
          !context.cancelled() &&
          context.remainingMs() !== 0
        ) {
          // Only the previous caller's wait failed. The component still owns
          // its physical drain; join it again using this caller's live context.
          return this.stopComponent(component, context, admission);
        }
        throw error;
      });
    }
    const operation: Promise<void> = Promise.resolve()
      .then(async () => {
        if (admissionOnly) await (component.lifecycle as AdmissionLifecycle).stopAdmission(context);
        else await component.lifecycle.stop(context);
      })
      .then(
        () => {
          if (!admissionOnly) {
            const index = this.#started.indexOf(component);
            if (index >= 0) this.#started.splice(index, 1);
            this.#admissionStops.delete(component);
          }
        },
        (error: unknown) => {
          if (pending.get(component)?.promise === operation) pending.delete(component);
          throw error;
        }
      );
    pending.set(component, { context, promise: operation });
    return operation;
  }

  private async stopConcurrent(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    await this.stopGroup(components, context, false);
  }

  private async stopAdmission(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    await this.stopGroup(components, context, true);
  }

  private async stopGroup(
    components: readonly RuntimeComponent[],
    context: Context,
    admission: boolean
  ): Promise<void> {
    const ordered = components.toReversed();
    const budget = context.remainingMs();
    const results = await settleWithinDeadline(
      context,
      ordered.map((component) => this.stopComponent(component, context, admission))
    );
    let failure: Error | undefined;
    for (const [index, component] of ordered.entries()) {
      const result = results[index];
      if (result === undefined) {
        this.logStopTimeout(context, component);
        failure ??= this.shutdownError(context, budget);
      } else if (result.status === "rejected") {
        this.logStopError(context, component, result.reason);
        failure ??= errorFromUnknown(result.reason);
      }
    }
    if (failure !== undefined) throw failure;
  }

  private async stopSequential(
    components: readonly RuntimeComponent[],
    context: Context
  ): Promise<void> {
    for (const component of components.toReversed()) {
      const budget = context.remainingMs();
      const [result] = await settleWithinDeadline(context, [
        this.stopComponent(component, context, false)
      ]);
      if (result === undefined) {
        this.logStopTimeout(context, component);
        throw this.shutdownError(context, budget);
      }
      if (result.status === "rejected") {
        this.logStopError(context, component, result.reason);
        throw errorFromUnknown(result.reason);
      }
    }
  }

  private shutdownError(context: Context, budget: number | undefined): Error {
    const reason: unknown = context.signal().reason;
    if (reason instanceof ContextDeadlineExceededError || context.remainingMs() === 0) {
      return new RuntimeDrainTimeoutError(budget ?? 0);
    }
    return errorFromUnknown(reason ?? new RuntimeStoppedError("runtime shutdown was cancelled"));
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
        err(errorFromUnknown(error))
      );
  }
}
