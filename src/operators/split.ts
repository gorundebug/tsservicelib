import {
  ConsumedStream,
  type Caller,
  type Completion,
  type MessageContext,
  type RuntimeBuildable,
  type RuntimeEnvironment,
  type SplitStreamConfig,
  type Stream,
  type StreamConfig,
  type StreamSerde,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/index.js";

class SplitLink<T> implements TypedStream<T> {
  readonly #split: SplitStream<T>;
  readonly #index: number;
  #consumer: TypedStreamConsumer<T> | undefined;
  #caller: Caller<T> | undefined;

  public constructor(split: SplitStream<T>, index: number) {
    this.#split = split;
    this.#index = index;
  }

  public get id(): number {
    return this.#split.id;
  }

  public get name(): string {
    return `${this.#split.name}SplitLink${String(this.#index)}`;
  }

  public get transformationName(): string {
    return this.#split.transformationName;
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    return this.#split.runtimeEnvironment();
  }

  public config(): StreamConfig {
    return this.#split.config();
  }

  public serde(): StreamSerde<T> {
    return this.#split.serde();
  }

  public typeName(): string {
    return this.#split.typeName();
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
    this.#caller = this.runtimeEnvironment().makeCaller(this, consumer);
  }

  public isAsync(): boolean {
    return this.#caller?.isAsync() ?? false;
  }

  public emit(context: MessageContext, value: T): Completion {
    return this.#caller?.consume(context, value);
  }
}

export class SplitStream<T>
  extends ConsumedStream<T>
  implements TypedStreamConsumer<T>, RuntimeBuildable
{
  readonly #links: SplitLink<T>[] = [];
  #dispatchOrder: readonly SplitLink<T>[] = [];

  public constructor(config: SplitStreamConfig, source: TypedStream<T>) {
    const environment = source.runtimeEnvironment();
    super(config, environment, source.serde());
    source.setConsumer(this);
    environment.registerStream(this);
    environment.registerRuntimeBuildable(this);
  }

  public addStream(): TypedStream<T> {
    const link = new SplitLink(this, this.#links.length);
    this.#links.push(link);
    return link;
  }

  public build(): void {
    for (const [index, link] of this.#links.entries()) {
      if (link.consumer() === undefined) {
        throw new Error(
          `link with index ${String(index)} for SplitStream ${this.name} does not have consumer`
        );
      }
    }
    this.#dispatchOrder = [...this.#links].sort(
      (left, right) => Number(right.isAsync()) - Number(left.isAsync())
    );
  }

  public override consumers(): readonly Stream[] {
    return this.#links.map((link, index) => {
      const consumer = link.consumer();
      if (consumer === undefined) {
        throw new Error(
          `link with index ${String(index)} for SplitStream ${this.name} does not have consumer`
        );
      }
      return consumer;
    });
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.emitLinks(context, value);
    }
    return this.traceCompletion(context, "stream.split", (spanContext) =>
      this.emitLinks(spanContext, value)
    );
  }

  private emitLinks(context: MessageContext, value: T): Completion {
    let pending: Promise<void> | undefined;
    for (const link of this.#dispatchOrder) {
      if (pending === undefined) {
        const completion = link.emit(context, value);
        if (completion !== undefined) pending = completion;
      } else {
        pending = pending.then(() => link.emit(context, value));
      }
    }
    return pending;
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}

export function makeSplitStream<T>(
  config: SplitStreamConfig,
  source: TypedStream<T>
): SplitStream<T> {
  return new SplitStream(config, source);
}
