import {
  ConsumedStream,
  type Completion,
  type MergeStreamConfig,
  type MessageContext,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/index.js";
import { StreamLink } from "./stream-link.js";

class MergeLink<T> extends StreamLink implements TypedStreamConsumer<T> {
  readonly #merge: MergeStream<T>;

  public constructor(merge: MergeStream<T>) {
    super(merge);
    this.#merge = merge;
  }

  public consume(context: MessageContext, value: T): Completion {
    return this.#merge.consume(context, value);
  }
}

export class MergeStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
  public constructor(
    config: MergeStreamConfig,
    sources: readonly [TypedStream<T>, ...TypedStream<T>[]]
  ) {
    const environment = sources[0].runtimeEnvironment();
    for (const source of sources) {
      if (source.runtimeEnvironment() !== environment) {
        throw new Error(`merge stream ${config.name} sources belong to different environments`);
      }
    }
    super(config, environment, sources[0].serde());
    environment.registerStream(this);
    for (const source of sources) {
      source.setConsumer(new MergeLink(this));
    }
  }

  public consume(context: MessageContext, value: T): Completion {
    if (!this.tracingEnabled(context)) {
      return this.emit(context, value);
    }
    return this.traceCompletion(context, "stream.merge", (spanContext) =>
      this.emit(spanContext, value)
    );
  }

  public functionImplementation(): undefined {
    return undefined;
  }
}

export function makeMergeStream<T>(
  config: MergeStreamConfig,
  source: TypedStream<T>,
  ...sources: TypedStream<T>[]
): MergeStream<T> {
  return new MergeStream(config, [source, ...sources]);
}
