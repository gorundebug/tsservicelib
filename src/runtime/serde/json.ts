import { appendBytes, validateLimits } from "./framing.js";
import { type SerdeLimits, SerdeError, unlimitedSerdeLimits, ValueSerde } from "./serde.js";
import type { SerdeType } from "./registry.js";

export class JsonSerde<T> extends ValueSerde<T> {
  readonly #encoder = new TextEncoder();
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });
  readonly #limits: SerdeLimits;

  public constructor(
    private readonly type: SerdeType<T>,
    limits: SerdeLimits = unlimitedSerdeLimits
  ) {
    super();
    validateLimits(limits);
    this.#limits = limits;
  }

  public serialize(value: T, prefix?: Uint8Array): Uint8Array {
    this.type.assert(value);
    let json: string | undefined;
    try {
      json = stringify(value);
    } catch (error: unknown) {
      throw new SerdeError(jsonErrorMessage("JSON serialization failed", error), 0);
    }
    if (json === undefined) {
      throw new SerdeError("JSON serialization produced no value", 0);
    }
    const encoded = this.#encoder.encode(json);
    if (encoded.byteLength > this.#limits.maxBytes) {
      throw new SerdeError("JSON output exceeds configured limit", 0);
    }
    return appendBytes(prefix, encoded);
  }

  public deserialize(data: Uint8Array): T {
    if (data.byteLength > this.#limits.maxTotalBytes || data.byteLength > this.#limits.maxBytes) {
      throw new SerdeError("JSON input exceeds configured limit", 0);
    }
    let json: string;
    try {
      json = this.#decoder.decode(data);
    } catch (error: unknown) {
      throw new SerdeError(jsonErrorMessage("JSON input is not valid UTF-8", error), 0);
    }
    let value: unknown;
    try {
      value = JSON.parse(json) as unknown;
    } catch (error: unknown) {
      throw new SerdeError(jsonErrorMessage("JSON parsing failed", error), jsonErrorOffset(error));
    }
    this.type.assert(value);
    return value;
  }
}

function stringify(value: unknown): string | undefined {
  return JSON.stringify(value);
}

function jsonErrorMessage(prefix: string, error: unknown): string {
  return `${prefix}${error instanceof Error ? `: ${error.message}` : ""}`;
}

function jsonErrorOffset(error: unknown): number {
  if (!(error instanceof SyntaxError)) {
    return 0;
  }
  const match = /position (\d+)/u.exec(error.message);
  if (match === null) {
    return 0;
  }
  const offset = Number(match[1]);
  return Number.isSafeInteger(offset) ? offset : 0;
}
