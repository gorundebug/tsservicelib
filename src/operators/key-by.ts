import {
  ConsumedStream,
  makeStreamKeyValueSerde,
  type Collector,
  type Completion,
  type KeyValue,
  type MessageContext,
  type KeyByStreamConfig,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/index.js";
import type { KeyByFunction } from "./functions.js";

export class KeyByStream<T, K, V>
  extends ConsumedStream<KeyValue<K, V>>
  implements TypedStreamConsumer<T>, Collector<KeyValue<K, V>>
{
  readonly #source: TypedStream<T>;
  readonly #function: KeyByFunction<T, K, V>;

  public constructor(
    config: KeyByStreamConfig,
    source: TypedStream<T>,
    function_: KeyByFunction<T, K, V>
  ) {
    const environment = source.runtimeEnvironment();
    super(
      config,
      environment,
      makeStreamKeyValueSerde(
        environment.serdeByName<K>(config.keyType),
        environment.serdeByName<V>(config.valueType)
      )
    );
    this.#source = source;
    this.#function = function_;
    source.setConsumer(this);
    this.runtimeEnvironment().registerStream(this);
  }

  public source(): TypedStream<T> {
    return this.#source;
  }

  public functionImplementation(): KeyByFunction<T, K, V> {
    return this.#function;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.#function.keyBy(context, this, value, this);
    }
    return this.traceCompletion(context, "stream.keyby", (spanContext) =>
      this.#function.keyBy(spanContext, this, value, this)
    );
  }
}

export function makeKeyByStream<T, K, V>(
  config: KeyByStreamConfig,
  source: TypedStream<T>,
  function_: KeyByFunction<T, K, V>
): KeyByStream<T, K, V> {
  return new KeyByStream(config, source, function_);
}
