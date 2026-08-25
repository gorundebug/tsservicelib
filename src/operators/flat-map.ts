import type { Collector } from "../runtime/collector.js";
import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { FlatMapStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { FlatMapFunction } from "./functions.js";

export class FlatMapStream<T, R>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<T>, Collector<R>
{
  readonly #source: TypedStream<T>;
  readonly #function: FlatMapFunction<T, R>;

  public constructor(
    config: FlatMapStreamConfig,
    source: TypedStream<T>,
    function_: FlatMapFunction<T, R>
  ) {
    const environment = source.runtimeEnvironment();
    super(config, environment, environment.serdeByName<R>(config.valueType));
    this.#source = source;
    this.#function = function_;
    source.setConsumer(this);
    this.runtimeEnvironment().registerStream(this);
  }

  public source(): TypedStream<T> {
    return this.#source;
  }

  public functionImplementation(): FlatMapFunction<T, R> {
    return this.#function;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.#function.flatMap(context, this, value, this);
    }
    return this.traceCompletion(context, "stream.flatmap", (spanContext) =>
      this.#function.flatMap(spanContext, this, value, this)
    );
  }
}

export function makeFlatMapStream<T, R>(
  config: FlatMapStreamConfig,
  source: TypedStream<T>,
  function_: FlatMapFunction<T, R>
): FlatMapStream<T, R> {
  return new FlatMapStream(config, source, function_);
}
