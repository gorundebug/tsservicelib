import type { MessageContext } from "./context.js";
import type { Caller } from "./stream.js";

/** Counts a graph link without loading any tracing runtime. */
export class CountedCaller<T> implements Caller<T> {
  public constructor(
    private readonly caller: Caller<T>,
    private readonly recordCall: (context: MessageContext) => void
  ) {}

  public isAsync(): boolean {
    return this.caller.isAsync();
  }

  public consume(context: MessageContext, value: T): void | Promise<void> {
    this.recordCall(context);
    return this.caller.consume(context, value);
  }
}
