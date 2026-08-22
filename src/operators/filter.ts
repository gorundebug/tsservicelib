import {
  ConsumedStream,
  type MessageContext,
  type FilterStreamConfig,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/index.js";
import type { FilterFunction } from "./functions.js";

export class FilterStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  readonly #source: TypedStream<T>;
  readonly #function: FilterFunction<T>;

  public constructor(
    config: FilterStreamConfig,
    source: TypedStream<T>,
    function_: FilterFunction<T>
  ) {
    super(config, source.runtimeEnvironment(), source.serde());
    this.#source = source;
    this.#function = function_;
    source.setConsumer(this);
    this.runtimeEnvironment().registerStream(this);
  }

  public source(): TypedStream<T> {
    return this.#source;
  }

  public functionImplementation(): FilterFunction<T> {
    return this.#function;
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    if (!this.tracingEnabled(context)) {
      await this.consumeFiltered(context, value);
      return;
    }
    await this.traceCompletion(context, "stream.filter", async (spanContext) => {
      await this.consumeFiltered(spanContext, value);
    });
  }

  private async consumeFiltered(context: MessageContext, value: T): Promise<void> {
    if (await this.#function.filter(context, this, value)) {
      await this.emit(context, value);
    }
  }
}

export function makeFilterStream<T>(
  config: FilterStreamConfig,
  source: TypedStream<T>,
  function_: FilterFunction<T>
): FilterStream<T> {
  return new FilterStream(config, source, function_);
}
