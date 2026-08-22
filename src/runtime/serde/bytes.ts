import { appendBytes, encodeSize, SerdeReader, validateLimits } from "./framing.js";
import { SerdeError, type SerdeLimits, unlimitedSerdeLimits, ValueSerde } from "./serde.js";

export class BytesSerde extends ValueSerde<Uint8Array> {
  readonly #limits: SerdeLimits;

  public constructor(limits: SerdeLimits = unlimitedSerdeLimits) {
    super();
    validateLimits(limits);
    this.#limits = limits;
  }

  public serialize(value: Uint8Array, prefix?: Uint8Array): Uint8Array {
    if (!(value instanceof Uint8Array)) {
      throw new TypeError("BytesSerde expects Uint8Array");
    }
    if (value.byteLength > this.#limits.maxBytes) {
      throw new SerdeError("bytes exceed configured limit", 0);
    }
    return appendBytes(prefix, appendBytes(encodeSize(value.byteLength), value));
  }

  public deserialize(data: Uint8Array): Uint8Array {
    const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
    const length = reader.readSize(this.#limits.maxBytes, "bytes length");
    return reader.read(length, "bytes");
  }
}

export class StringSerde extends ValueSerde<string> {
  readonly #limits: SerdeLimits;
  readonly #encoder = new TextEncoder();
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });

  public constructor(limits: SerdeLimits = unlimitedSerdeLimits) {
    super();
    validateLimits(limits);
    this.#limits = limits;
  }

  public serialize(value: string, prefix?: Uint8Array): Uint8Array {
    if (typeof value !== "string") {
      throw new TypeError("StringSerde expects a string");
    }
    const encoded = this.#encoder.encode(value);
    if (encoded.byteLength > this.#limits.maxStringBytes) {
      throw new SerdeError("string exceeds configured limit", 0);
    }
    return appendBytes(prefix, appendBytes(encodeSize(encoded.byteLength), encoded));
  }

  public deserialize(data: Uint8Array): string {
    const reader = new SerdeReader(data, this.#limits.maxTotalBytes);
    const lengthOffset = reader.offset;
    const length = reader.readSize(this.#limits.maxStringBytes, "string length");
    const payload = reader.read(length, "string");
    try {
      return this.#decoder.decode(payload);
    } catch (error: unknown) {
      throw new SerdeError(
        `invalid UTF-8 string${error instanceof Error ? `: ${error.message}` : ""}`,
        lengthOffset + 8
      );
    }
  }
}
