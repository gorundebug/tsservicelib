export class SerdeError extends Error {
  public constructor(
    message: string,
    public readonly offset: number
  ) {
    super(`${message} at byte ${String(offset)}`);
    this.name = "SerdeError";
  }
}

export interface SerdeLimits {
  readonly maxStringBytes: number;
  readonly maxBytes: number;
  readonly maxContainerElements: number;
  readonly maxTotalBytes: number;
}

export const unlimitedSerdeLimits: Readonly<SerdeLimits> = Object.freeze({
  maxStringBytes: Number.MAX_SAFE_INTEGER,
  maxBytes: Number.MAX_SAFE_INTEGER,
  maxContainerElements: Number.MAX_SAFE_INTEGER,
  maxTotalBytes: Number.MAX_SAFE_INTEGER
});

export interface Serde<T> {
  serialize(value: T, prefix?: Uint8Array): Uint8Array;
  deserialize(data: Uint8Array): T;
  isStub(): boolean;
}

export interface StreamSerde<T> extends Serde<T> {
  typeName(): string;
  isKeyValue(): boolean;
  serializeKey(value: T): Uint8Array | undefined;
  serializeValue(value: T): Uint8Array;
  deserializeKeyValue(key: Uint8Array | undefined, value: Uint8Array): T;
}

export abstract class ValueSerde<T> implements Serde<T> {
  public abstract serialize(value: T, prefix?: Uint8Array): Uint8Array;
  public abstract deserialize(data: Uint8Array): T;

  public isStub(): boolean {
    return false;
  }
}

export class StubSerde<T> implements Serde<T> {
  public serialize(value: T, prefix?: Uint8Array): Uint8Array {
    void value;
    void prefix;
    throw new SerdeError("stub serde cannot serialize", 0);
  }

  public deserialize(data: Uint8Array): T {
    void data;
    throw new SerdeError("stub serde cannot deserialize", 0);
  }

  public isStub(): boolean {
    return true;
  }
}
