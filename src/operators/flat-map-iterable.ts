import {
  ConsumedStream,
  type FlatMapIterableStreamConfig,
  type MessageContext,
  type RuntimeEnvironment,
  type TypedStream,
  type TypedStreamConsumer
} from "../runtime/index.js";

/** Arrays and typed arrays have the indexed semantics supported by Go. */
export interface IndexedIterable<T> extends Iterable<T> {
  readonly length: number;
  readonly [index: number]: T;
}

export type FlatMapIterableInput<T> = IndexedIterable<T> | string;

const utf8Encoder = new TextEncoder();

function stringItems(value: string, valueType: string): Iterable<number> {
  if (valueType === "int32") {
    return (function* codePoints(): Generator<number> {
      for (const character of value) {
        const codePoint = character.codePointAt(0);
        if (codePoint === undefined) {
          throw new Error("string iteration produced an empty character");
        }
        yield codePoint;
      }
    })();
  }
  if (valueType === "uint8") {
    return utf8Encoder.encode(value);
  }
  throw new TypeError(
    `FlatMapIterable string output type must be int32 or uint8, got ${valueType}`
  );
}

function isIndexedIterable<T>(value: unknown): value is IndexedIterable<T> {
  if (
    typeof value !== "object" ||
    value === null ||
    !(Symbol.iterator in value) ||
    !("length" in value)
  ) {
    return false;
  }
  const length = value.length;
  return typeof length === "number" && Number.isSafeInteger(length) && length >= 0;
}

export class FlatMapIterableStream<T extends FlatMapIterableInput<R>, R>
  extends ConsumedStream<R>
  implements TypedStreamConsumer<T>
{
  readonly #source: TypedStream<T>;
  readonly #valueType: string;

  public constructor(config: FlatMapIterableStreamConfig, source: TypedStream<T>) {
    super(
      config,
      source.runtimeEnvironment(),
      source.runtimeEnvironment().serdeByName<R>(config.valueType)
    );
    this.#source = source;
    this.#valueType = config.valueType;
    source.setConsumer(this);
    this.runtimeEnvironment().registerStream(this);
  }

  public source(): TypedStream<T> {
    return this.#source;
  }

  public functionImplementation(): undefined {
    return undefined;
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    if (!this.tracingEnabled(context)) {
      await this.emitItems(context, value);
      return;
    }
    await this.traceCompletion(context, "stream.flatmapiterable", async (spanContext) => {
      await this.emitItems(spanContext, value);
    });
  }

  private async emitItems(context: MessageContext, value: T): Promise<void> {
    if (typeof value === "string") {
      for (const item of stringItems(value, this.#valueType)) {
        const output: unknown = item;
        const environment: RuntimeEnvironment = this.runtimeEnvironment();
        environment.assertSerdeValue<R>(this.#valueType, output);
        await this.emit(context, output);
      }
      return;
    }
    if (!isIndexedIterable<R>(value)) {
      throw new TypeError(`FlatMapIterable stream ${this.name} requires an array or typed array`);
    }
    await this.emitIndexed(context, value);
  }

  private async emitIndexed(context: MessageContext, value: IndexedIterable<R>): Promise<void> {
    for (const item of value) {
      await this.emit(context, item);
    }
  }
}

export function makeFlatMapIterableStream(
  config: FlatMapIterableStreamConfig & { readonly valueType: "int32" | "uint8" },
  source: TypedStream<string>
): FlatMapIterableStream<string, number>;
export function makeFlatMapIterableStream<R>(
  config: FlatMapIterableStreamConfig,
  source: TypedStream<IndexedIterable<R>>
): FlatMapIterableStream<IndexedIterable<R>, R>;
export function makeFlatMapIterableStream<R>(
  config: FlatMapIterableStreamConfig,
  source: TypedStream<FlatMapIterableInput<R>>
): FlatMapIterableStream<FlatMapIterableInput<R>, R> {
  return new FlatMapIterableStream(config, source);
}
