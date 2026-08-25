import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { InputStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { KeyValue } from "../runtime/datastruct/key-value.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import type { Completion, Consumer, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import { ErrorStream } from "./error.js";
import { StreamLink } from "./stream-link.js";

/** Internal feedback edge. Its graph identity is the owning input stream. */
class ResultLink<T, R, E> extends StreamLink implements TypedStreamConsumer<R> {
  readonly #input: InputStream<T, R, E>;

  public constructor(input: InputStream<T, R, E>) {
    super(input);
    this.#input = input;
  }

  public consume(context: MessageContext, value: R): Completion {
    return this.#input.consumeResult(context, value);
  }
}

export class InputStream<T, R, E> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  readonly #endpointId: number;
  readonly #errorStream: ErrorStream<E>;
  #resultSource: TypedStream<R> | undefined;
  #resultConsumer: Consumer<R> | undefined;

  public constructor(
    config: InputStreamConfig,
    environment: RuntimeEnvironment,
    valueSerde: StreamSerde<T>,
    errorSerde: StreamSerde<E>
  ) {
    super(config, environment, valueSerde);
    this.#endpointId = config.idEndpoint;
    this.#errorStream = new ErrorStream(config, environment, errorSerde, this);
    environment.registerStream(this);
  }

  public endpointId(): number {
    return this.#endpointId;
  }

  public errorStream(): ErrorStream<E> {
    return this.#errorStream;
  }

  public resultStream(): TypedStream<R> | undefined {
    return this.#resultSource;
  }

  public setSource(source: TypedStream<R>): void {
    if (this.#resultSource !== undefined) {
      throw new Error(`input stream ${this.name} result source is already set`);
    }
    source.setConsumer(new ResultLink(this));
    this.#resultSource = source;
  }

  public setResultConsumer(consumer: Consumer<R>): void {
    this.#resultConsumer = consumer;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.emit(context, value);
    }
    return this.traceCompletion(context, "stream.input", (spanContext) =>
      this.emit(spanContext, value)
    );
  }

  public consumeError(context: MessageContext, value: E): Completion {
    return this.#errorStream.emit(context, value);
  }

  public consumeResult(context: MessageContext, value: R): Completion {
    return this.#resultConsumer?.consume(context, value);
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}

export class InputKVStream<K, V, R, E> extends InputStream<KeyValue<K, V>, R, E> {}

export function makeInputStream<T, R, E>(
  config: InputStreamConfig,
  environment: RuntimeEnvironment
): InputStream<T, R, E> {
  return new InputStream(
    config,
    environment,
    environment.serdeByName<T>(config.valueType),
    environment.streamErrorSerde<E>(config.id)
  );
}

export function makeInputKVStream<K, V, R, E>(
  config: InputStreamConfig,
  environment: RuntimeEnvironment
): InputKVStream<K, V, R, E> {
  const valueSerde = environment.serdeByName<KeyValue<K, V>>(config.valueType);
  if (!valueSerde.isKeyValue()) {
    throw new Error(`input stream ${config.name} valueType ${config.valueType} is not key-value`);
  }
  return new InputKVStream(
    config,
    environment,
    valueSerde,
    environment.streamErrorSerde<E>(config.id)
  );
}
