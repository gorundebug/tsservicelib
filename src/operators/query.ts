import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { QueryStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import {
  type Completion,
  type Consumer,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/stream.js";
import { ErrorStream } from "./error.js";

export class QueryStream<T, R, E>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<T>
{
  readonly #endpointId: number;
  readonly #errorStream: ErrorStream<E>;
  readonly #inputSerde: StreamSerde<T>;
  #queryConsumer: Consumer<T> | undefined;

  public constructor(config: QueryStreamConfig, source: TypedStream<T>) {
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

  public setQueryConsumer(consumer: Consumer<T>): void {
    this.#queryConsumer = consumer;
  }

  public inputSerde(): StreamSerde<T> {
    return this.#inputSerde;
  }

  public consume(context: MessageContext, value: T): Completion {
    const endpoint = this.runtimeEnvironment().runtimeConfig().endpointById(this.#endpointId);
    if (endpoint !== undefined && "enabled" in endpoint && endpoint.enabled === false) return;
    const consumer = this.#queryConsumer;
    if (consumer === undefined) {
      return;
    }
    if (!this.tracingEnabled(context)) {
      return consumer.consume(context, value);
    }
    return this.traceCompletion(context, "stream.query", (spanContext) =>
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

function requireResultType(config: QueryStreamConfig): string {
  if (config.valueType === undefined) {
    throw new Error(`query stream ${config.name} result valueType is missing`);
  }
  return config.valueType;
}

export function makeQueryStream<T, R, E>(config: QueryStreamConfig, source: TypedStream<T>): QueryStream<T, R, E> {
  return new QueryStream(config, source);
}
