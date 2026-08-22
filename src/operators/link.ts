import {
  ConsumedStream,
  type Completion,
  type CycleLinkStreamConfig,
  type MessageContext,
  type RuntimeEnvironment,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/index.js";

/** A graph root whose source is connected after the acyclic graph is built. */
export class LinkStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  #source: TypedStream<T> | undefined;

  public constructor(config: CycleLinkStreamConfig, environment: RuntimeEnvironment) {
    super(config, environment, environment.streamValueSerde<T>(config.id));
    environment.registerStream(this);
  }

  public source(): TypedStream<T> | undefined {
    return this.#source;
  }

  public setSource(source: TypedStream<T>): void {
    if (this.#source !== undefined) {
      throw new Error(`cycle link stream ${this.name} source is already set`);
    }
    source.setConsumer(this);
    this.#source = source;
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.emit(context, value);
    }
    return this.traceCompletion(context, "stream.link", (spanContext) =>
      this.emit(spanContext, value)
    );
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}

export function makeLinkStream<T>(
  config: CycleLinkStreamConfig,
  environment: RuntimeEnvironment
): LinkStream<T> {
  return new LinkStream(config, environment);
}
