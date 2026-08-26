import { RuntimeDrainTimeoutError, RuntimeStoppedError, errorFromUnknown } from "./errors.js";
import { combineAbortSignals } from "./context.js";

export type RuntimeTask<T> = (signal: AbortSignal) => Promise<T>;
export type RuntimeTaskErrorHandler = (error: Error) => void;

export class RuntimeTaskRegistry {
  readonly #controller = new AbortController();
  readonly #tasks = new Map<number, Promise<unknown>>();
  readonly #onError: RuntimeTaskErrorHandler;
  #accepting = true;
  #nextId = 1;

  public constructor(onError: RuntimeTaskErrorHandler = () => undefined) {
    this.#onError = onError;
  }

  public accepting(): boolean {
    return this.#accepting;
  }

  public activeCount(): number {
    return this.#tasks.size;
  }

  public admit<T>(task: RuntimeTask<T>, externalSignal?: AbortSignal): Promise<T> {
    if (!this.#accepting) {
      return Promise.reject<T>(new RuntimeStoppedError());
    }

    let admitted: Promise<T> | undefined;
    this.startTask(task, externalSignal, (promise) => {
      admitted = promise;
    });
    if (admitted === undefined) {
      throw new Error("runtime task admission did not produce a promise");
    }
    return admitted;
  }

  public admitDetached<T>(task: RuntimeTask<T>, externalSignal?: AbortSignal): void {
    if (!this.#accepting) {
      this.#onError(new RuntimeStoppedError());
      return;
    }
    this.startTask(task, externalSignal, () => undefined);
  }

  private startTask<T>(
    task: RuntimeTask<T>,
    externalSignal: AbortSignal | undefined,
    accepted: (promise: Promise<T>) => void
  ): void {
    const id = this.#nextId++;
    const signal =
      externalSignal === undefined
        ? this.#controller.signal
        : combineAbortSignals([this.#controller.signal, externalSignal]);
    let promise: Promise<T>;
    try {
      promise = task(signal);
    } catch (error: unknown) {
      promise = Promise.reject(errorFromUnknown(error));
    }
    accepted(promise);
    const observed = promise.then(
      () => {
        this.#tasks.delete(id);
      },
      (value: unknown) => {
        // The task promise is already observed at this boundary. A faulty
        // reporting callback must not manufacture a second unhandled
        // rejection while the original promise remains visible to admit().
        try {
          this.#onError(errorFromUnknown(value));
        } catch {
          // Error reporters are terminal sinks. There is no second canonical
          // destination to which their own failure could be routed safely.
        }
        this.#tasks.delete(id);
      }
    );
    this.#tasks.set(id, observed);
  }

  public stopAdmission(): void {
    this.#accepting = false;
  }

  public cancel(reason: unknown = new RuntimeStoppedError("runtime work was cancelled")): void {
    this.#accepting = false;
    this.#controller.abort(reason);
  }

  public async drain(timeoutMs?: number): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    const timeout =
      timeoutMs === undefined
        ? undefined
        : new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => {
              reject(new RuntimeDrainTimeoutError(timeoutMs));
            }, timeoutMs);
          });
    try {
      while (this.#tasks.size > 0) {
        const snapshot = Promise.allSettled([...this.#tasks.values()]).then(() => undefined);
        if (timeout === undefined) {
          await snapshot;
          continue;
        }
        await Promise.race([snapshot, timeout]);
      }
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }
}
