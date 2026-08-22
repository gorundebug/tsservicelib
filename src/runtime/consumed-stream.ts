import { makeCollector, type Collector } from "./collector.js";
import type { MessageContext } from "./context.js";
import type { StreamConfig } from "./config/index.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import type { StreamSerde } from "./serde/index.js";
import {
  ServiceStream,
  type Caller,
  type Completion,
  type Stream,
  type TypedStream,
  type TypedStreamConsumer
} from "./stream.js";

export class ConsumedStream<T> extends ServiceStream implements TypedStream<T>, Collector<T> {
  #downstream: Caller<T> | undefined;
  #consumer: TypedStreamConsumer<T> | undefined;
  readonly #serde: StreamSerde<T>;

  public constructor(config: StreamConfig, environment: RuntimeEnvironment, serde: StreamSerde<T>) {
    super(config, environment);
    this.#serde = serde;
  }

  public serde(): StreamSerde<T> {
    return this.#serde;
  }

  public typeName(): string {
    return this.#serde.typeName();
  }

  public consumer(): TypedStreamConsumer<T> | undefined {
    return this.#consumer;
  }

  public consumers(): readonly Stream[] {
    return this.#consumer === undefined ? [] : [this.#consumer];
  }

  public setConsumer(consumer: TypedStreamConsumer<T>): void {
    if (this.#consumer !== undefined) {
      throw new Error(`consumer already assigned to stream ${this.name}`);
    }
    this.#consumer = consumer;
    this.#downstream = this.runtimeEnvironment().makeCaller(this, consumer);
  }

  public emit(context: MessageContext, value: T): Completion {
    return this.#downstream?.consume(context, value);
  }

  public out(context: MessageContext, value: T): Completion {
    return this.emit(context, value);
  }

  public collector(): Collector<T> {
    return makeCollector(this.#downstream);
  }
}
