import type { MessageContext } from "./context.js";
import type { PriorityTaskPoolLike, TaskPoolLike } from "./pool/pool.js";
import type { Caller, Consumer } from "./stream.js";
import type { RuntimeTaskRegistry } from "./task-registry.js";

export type CallerRejectionHandler = (error: unknown) => void;

export class TaskPoolCaller<T> implements Caller<T> {
  readonly #pool: TaskPoolLike;
  readonly #consumer: Consumer<T>;
  readonly #onRejected: CallerRejectionHandler;

  public constructor(
    pool: TaskPoolLike,
    consumer: Consumer<T>,
    onRejected: CallerRejectionHandler = () => undefined
  ) {
    this.#pool = pool;
    this.#consumer = consumer;
    this.#onRejected = onRejected;
  }

  public isAsync(): boolean {
    return true;
  }

  public consume(context: MessageContext, value: T): void {
    try {
      this.#pool.addTask(context, () => this.#consumer.consume(context, value));
    } catch (error: unknown) {
      this.#onRejected(error);
    }
  }
}

export class PriorityTaskPoolCaller<T> implements Caller<T> {
  readonly #pool: PriorityTaskPoolLike;
  readonly #consumer: Consumer<T>;
  readonly #defaultPriority: number;
  readonly #onRejected: CallerRejectionHandler;

  public constructor(
    pool: PriorityTaskPoolLike,
    consumer: Consumer<T>,
    defaultPriority: number,
    onRejected: CallerRejectionHandler = () => undefined
  ) {
    this.#pool = pool;
    this.#consumer = consumer;
    this.#defaultPriority = defaultPriority;
    this.#onRejected = onRejected;
  }

  public isAsync(): boolean {
    return true;
  }

  public consume(context: MessageContext, value: T): void {
    try {
      this.#pool.addTask(context, context.priority() ?? this.#defaultPriority, () =>
        this.#consumer.consume(context, value)
      );
    } catch (error: unknown) {
      this.#onRejected(error);
    }
  }
}

export class ParallelCaller<T> implements Caller<T> {
  readonly #tasks: RuntimeTaskRegistry;
  readonly #consumer: Consumer<T>;

  public constructor(tasks: RuntimeTaskRegistry, consumer: Consumer<T>) {
    this.#tasks = tasks;
    this.#consumer = consumer;
  }

  public isAsync(): boolean {
    return true;
  }

  public consume(context: MessageContext, value: T): void {
    this.#tasks.admitDetached(async (signal) => {
      await this.#consumer.consume(context.withExternalCancellation(signal), value);
    });
  }
}
