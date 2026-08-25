import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { SinkStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import {
  ServiceStream,
  type Completion,
  type Consumer,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/stream.js";
import { ErrorStream } from "./error.js";

export class SinkStream<T, E> extends ServiceStream implements TypedStreamConsumer<T> {
  readonly #endpointId: number;
  readonly #errorStream: ErrorStream<E>;
  readonly #inputSerde: StreamSerde<T>;
  #sinkConsumer: Consumer<T> | undefined;

  public constructor(config: SinkStreamConfig, source: TypedStream<T>) {
    const environment = source.runtimeEnvironment();
    super(config, environment);
    this.#endpointId = config.idEndpoint;
    this.#inputSerde = source.serde();
    this.#errorStream = new ErrorStream(
      config,
      environment,
      environment.streamErrorSerde(config.id),
      this
    );
    source.setConsumer(this);
    environment.registerStream(this);
  }

  public endpointId(): number {
    return this.#endpointId;
  }

  public errorStream(): ErrorStream<E> {
    return this.#errorStream;
  }

  public setSinkConsumer(consumer: Consumer<T>): void {
    this.#sinkConsumer = consumer;
  }

  public inputSerde(): StreamSerde<T> {
    return this.#inputSerde;
  }

  public consume(context: MessageContext, value: T): Completion {
    const consumer = this.#sinkConsumer;
    if (consumer === undefined) {
      return;
    }
    if (!this.tracingEnabled(context)) {
      return consumer.consume(context, value);
    }
    return this.traceCompletion(context, "stream.sink", (spanContext) =>
      consumer.consume(spanContext, value)
    );
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}

export class SinkStreamWithResult<T, R, E>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<T>
{
  readonly #endpointId: number;
  readonly #errorStream: ErrorStream<E>;
  readonly #inputSerde: StreamSerde<T>;
  #sinkConsumer: Consumer<T> | undefined;

  public constructor(config: SinkStreamConfig, source: TypedStream<T>) {
    const environment = source.runtimeEnvironment();
    super(config, environment, environment.serdeByName<R>(requireResultType(config)));
    this.#endpointId = config.idEndpoint;
    this.#inputSerde = source.serde();
    this.#errorStream = new ErrorStream(
      config,
      environment,
      environment.streamErrorSerde(config.id),
      this
    );
    source.setConsumer(this);
    environment.registerStream(this);
  }

  public endpointId(): number {
    return this.#endpointId;
  }

  public errorStream(): ErrorStream<E> {
    return this.#errorStream;
  }

  public setSinkConsumer(consumer: Consumer<T>): void {
    this.#sinkConsumer = consumer;
  }

  public inputSerde(): StreamSerde<T> {
    return this.#inputSerde;
  }

  public consume(context: MessageContext, value: T): Completion {
    const consumer = this.#sinkConsumer;
    if (consumer === undefined) {
      return;
    }
    if (!this.tracingEnabled(context)) {
      return consumer.consume(context, value);
    }
    return this.traceCompletion(context, "stream.sink", (spanContext) =>
      consumer.consume(spanContext, value)
    );
  }

  public consumeResult(context: MessageContext, value: R): Completion {
    return this.emit(context, value);
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}

function requireResultType(config: SinkStreamConfig): string {
  if (config.valueType === undefined) {
    throw new Error(`sink stream ${config.name} result valueType is missing`);
  }
  return config.valueType;
}

export function makeSinkStream<T, E>(
  config: SinkStreamConfig,
  source: TypedStream<T>
): SinkStream<T, E> {
  return new SinkStream(config, source);
}

export function makeSinkStreamWithResult<T, R, E>(
  config: SinkStreamConfig,
  source: TypedStream<T>
): SinkStreamWithResult<T, R, E> {
  return new SinkStreamWithResult(config, source);
}
