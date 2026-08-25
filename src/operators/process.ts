import type { Collector } from "../runtime/collector.js";
import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { ProcessStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import { ErrorStream } from "./error.js";
import type { ProcessFunction } from "./functions.js";

export class ProcessStream<T, R, E>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<T>, Collector<R>
{
  readonly #source: TypedStream<T>;
  readonly #function: ProcessFunction<T, R, E>;
  readonly #errorStream: ErrorStream<E>;

  public constructor(
    config: ProcessStreamConfig,
    source: TypedStream<T>,
    function_: ProcessFunction<T, R, E>
  ) {
    const environment = source.runtimeEnvironment();
    super(config, environment, environment.streamValueSerde<R>(config.id));
    this.#source = source;
    this.#function = function_;
    this.#errorStream = new ErrorStream(
      config,
      environment,
      environment.streamErrorSerde(config.id),
      this
    );
    source.setConsumer(this);
    this.runtimeEnvironment().registerStream(this);
  }

  public source(): TypedStream<T> {
    return this.#source;
  }

  public errorStream(): ErrorStream<E> {
    return this.#errorStream;
  }

  public functionImplementation(): ProcessFunction<T, R, E> {
    return this.#function;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.#function.process(context, this, value, this, this.#errorStream);
    }
    return this.traceCompletion(context, "stream.process", (spanContext) =>
      this.#function.process(spanContext, this, value, this, this.#errorStream)
    );
  }
}

export function makeProcessStream<T, R, E>(
  config: ProcessStreamConfig,
  source: TypedStream<T>,
  function_: ProcessFunction<T, R, E>
): ProcessStream<T, R, E> {
  return new ProcessStream(config, source, function_);
}
