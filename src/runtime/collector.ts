import type { MessageContext } from "./context.js";
import type { Caller, Completion } from "./stream.js";

export interface Collector<T> {
  out(context: MessageContext, value: T): Completion;
}

export type CollectFunction<T> = (context: MessageContext, value: T) => Completion;

export class FunctionCollector<T> implements Collector<T> {
  readonly #collect: CollectFunction<T>;

  public constructor(collect: CollectFunction<T>) {
    this.#collect = collect;
  }

  public out(context: MessageContext, value: T): Completion {
    return this.#collect(context, value);
  }
}

export class CallerCollector<T> implements Collector<T> {
  readonly #caller: Caller<T> | undefined;

  public constructor(caller?: Caller<T>) {
    this.#caller = caller;
  }

  public out(context: MessageContext, value: T): Completion {
    return this.#caller?.consume(context, value);
  }
}

export function makeCollector<T>(caller?: Caller<T>): Collector<T> {
  return new CallerCollector(caller);
}
