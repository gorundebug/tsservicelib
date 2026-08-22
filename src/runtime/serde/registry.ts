import type { StreamSerde } from "./serde.js";

const registerSerde = Symbol("registerSerde");
const resolveSerde = Symbol("resolveSerde");

export type RuntimeTypePredicate<T> = (value: unknown) => value is T;

export class SerdeType<T> {
  readonly #registered = new WeakMap<SerdeRegistry, StreamSerde<T>>();

  public constructor(
    public readonly name: string,
    private readonly predicate: RuntimeTypePredicate<T>
  ) {
    if (name.trim().length === 0) {
      throw new Error("serde type name must not be empty");
    }
  }

  public is(value: unknown): value is T {
    return this.predicate(value);
  }

  public assert(value: unknown): asserts value is T {
    if (!this.predicate(value)) {
      throw new TypeError(`value is not ${this.name}`);
    }
  }

  public [registerSerde](registry: SerdeRegistry, serde: StreamSerde<T>): void {
    this.#registered.set(registry, serde);
  }

  public [resolveSerde](registry: SerdeRegistry): StreamSerde<T> | undefined {
    return this.#registered.get(registry);
  }
}

export class SerdeRegistry {
  readonly #byName = new Map<string, StreamSerde<unknown>>();
  readonly #assertByName = new Map<string, (value: unknown) => void>();
  readonly #streamValueTypes = new Map<number, string>();
  readonly #streamErrorTypes = new Map<number, string>();

  public register<T>(type: SerdeType<T>, serde: StreamSerde<T>): void {
    if (this.#byName.has(type.name)) {
      throw new Error(`serde type ${type.name} is already registered`);
    }
    const validated = new RuntimeValidatedStreamSerde(type, serde);
    this.#byName.set(type.name, validated);
    this.#assertByName.set(type.name, (value) => {
      type.assert(value);
    });
    type[registerSerde](this, validated);
  }

  public get<T>(type: SerdeType<T>): StreamSerde<T> | undefined {
    return type[resolveSerde](this);
  }

  public require<T>(type: SerdeType<T>): StreamSerde<T> {
    const serde = this.get(type);
    if (serde === undefined) {
      throw new Error(`serde type ${type.name} is not registered`);
    }
    return serde;
  }

  /** Resolve graph type metadata after TypeScript generic types have been erased. */
  public requireByName<T>(name: string): StreamSerde<T> {
    const serde = this.#byName.get(name);
    if (serde === undefined) {
      throw new Error(`serde type ${name} is not registered`);
    }
    // Every registry entry is guarded by RuntimeValidatedStreamSerde. The cast is
    // confined to this runtime boundary; invalid values still fail validation.
    return serde as StreamSerde<T>;
  }

  public matchesByName(name: string, value: unknown): boolean {
    const assert = this.#assertByName.get(name);
    if (assert === undefined) {
      throw new Error(`serde type ${name} is not registered`);
    }
    try {
      assert(value);
      return true;
    } catch (error) {
      if (error instanceof TypeError) return false;
      throw error;
    }
  }

  // The type parameter reifies graph metadata at the single erased runtime boundary.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
  public assertByName<T>(name: string, value: unknown): asserts value is T {
    const assert = this.#assertByName.get(name);
    if (assert === undefined) {
      throw new Error(`serde type ${name} is not registered`);
    }
    assert(value);
  }

  public registerStreamErrorType<T>(streamId: number, type: SerdeType<T>): void {
    if (this.#streamErrorTypes.has(streamId)) {
      throw new Error(`error serde for stream ${String(streamId)} is already registered`);
    }
    this.#streamErrorTypes.set(streamId, type.name);
  }

  /** Registers generated graph type metadata lost to JavaScript type erasure. */
  public registerStreamValueType<T>(streamId: number, type: SerdeType<T>): void {
    if (this.#streamValueTypes.has(streamId)) {
      throw new Error(`value serde for stream ${String(streamId)} is already registered`);
    }
    this.#streamValueTypes.set(streamId, type.name);
  }

  public requireStreamValue<T>(streamId: number): StreamSerde<T> {
    const name = this.#streamValueTypes.get(streamId);
    if (name === undefined) {
      throw new Error(`value serde for stream ${String(streamId)} is not registered`);
    }
    return this.requireByName<T>(name);
  }

  public requireStreamError<T>(streamId: number): StreamSerde<T> {
    const name = this.#streamErrorTypes.get(streamId);
    if (name === undefined) {
      throw new Error(`error serde for stream ${String(streamId)} is not registered`);
    }
    return this.requireByName<T>(name);
  }
}

class RuntimeValidatedStreamSerde<T> implements StreamSerde<T> {
  public constructor(
    private readonly type: SerdeType<T>,
    private readonly serde: StreamSerde<T>
  ) {}

  public serialize(value: T, prefix?: Uint8Array): Uint8Array {
    this.type.assert(value);
    return this.serde.serialize(value, prefix);
  }

  public deserialize(data: Uint8Array): T {
    return this.validate(this.serde.deserialize(data));
  }

  public isStub(): boolean {
    return this.serde.isStub();
  }

  public typeName(): string {
    return this.type.name;
  }

  public isKeyValue(): boolean {
    return this.serde.isKeyValue();
  }

  public serializeKey(value: T): Uint8Array | undefined {
    this.type.assert(value);
    return this.serde.serializeKey(value);
  }

  public serializeValue(value: T): Uint8Array {
    this.type.assert(value);
    return this.serde.serializeValue(value);
  }

  public deserializeKeyValue(key: Uint8Array | undefined, value: Uint8Array): T {
    return this.validate(this.serde.deserializeKeyValue(key, value));
  }

  private validate(value: unknown): T {
    this.type.assert(value);
    return value;
  }
}
