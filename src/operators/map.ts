import type { Collector } from "../runtime/collector.js";
import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { MapStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { MapFunction } from "./functions.js";

export class MapStream<T, R>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<T>, Collector<R>
{
  readonly #source: TypedStream<T>;
  readonly #function: MapFunction<T, R>;

  public constructor(
    config: MapStreamConfig,
    source: TypedStream<T>,
    function_: MapFunction<T, R>
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

  public functionImplementation(): MapFunction<T, R> {
    return this.#function;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.#function.map(context, this, value, this);
    }
    return this.traceCompletion(context, "stream.map", (spanContext) =>
      this.#function.map(spanContext, this, value, this)
    );
  }
}

export function makeMapStream<T, R>(
  config: MapStreamConfig,
  source: TypedStream<T>,
  function_: MapFunction<T, R>
): MapStream<T, R> {
  return new MapStream(config, source, function_);
}
