import type { KeyValue } from "../datastruct/index.js";
import { appendBytes, encodeSize, SerdeReader } from "./framing.js";
import { SerdeError, type Serde, type StreamSerde } from "./serde.js";

class ValueStreamSerde<T> implements StreamSerde<T> {
  public constructor(private readonly valueSerde: Serde<T>) {}

  public serialize(value: T, prefix?: Uint8Array): Uint8Array {
    return this.valueSerde.serialize(value, prefix);
  }

  public deserialize(data: Uint8Array): T {
    return this.valueSerde.deserialize(data);
  }

  public isStub(): boolean {
    return this.valueSerde.isStub();
  }

  public typeName(): string {
    return "";
  }

  public isKeyValue(): boolean {
    return false;
  }

  public serializeKey(value: T): undefined {
    void value;
    return undefined;
  }

  public serializeValue(value: T): Uint8Array {
    return this.valueSerde.serialize(value);
  }

  public deserializeKeyValue(_key: Uint8Array | undefined, value: Uint8Array): T {
    return this.valueSerde.deserialize(value);
  }
}

export class StreamKeyValueSerde<K, V> implements StreamSerde<KeyValue<K, V>> {
  public constructor(
    private readonly keySerde: Serde<K>,
    private readonly valueSerde: Serde<V>
  ) {}

  public serialize(value: KeyValue<K, V>, prefix?: Uint8Array): Uint8Array {
    const key = this.keySerde.serialize(value.key);
    const encodedValue = this.valueSerde.serialize(value.value);
    let output = appendBytes(prefix, encodeSize(key.byteLength));
    output = appendBytes(output, key);
    output = appendBytes(output, encodeSize(encodedValue.byteLength));
    return appendBytes(output, encodedValue);
  }

  public deserialize(data: Uint8Array): KeyValue<K, V> {
    const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
    const keyLength = reader.readSize(data.byteLength, "key length");
    const key = this.keySerde.deserialize(reader.read(keyLength, "key"));
    const valueLength = reader.readSize(data.byteLength, "value length");
    const value = this.valueSerde.deserialize(reader.read(valueLength, "value"));
    return { key, value };
  }

  public isStub(): boolean {
    return this.keySerde.isStub() || this.valueSerde.isStub();
  }

  public typeName(): string {
    return "";
  }

  public isKeyValue(): boolean {
    return true;
  }

  public serializeKey(value: KeyValue<K, V>): Uint8Array {
    return this.keySerde.serialize(value.key);
  }

  public serializeValue(value: KeyValue<K, V>): Uint8Array {
    return this.valueSerde.serialize(value.value);
  }

  public deserializeKeyValue(key: Uint8Array | undefined, value: Uint8Array): KeyValue<K, V> {
    if (key === undefined) {
      throw new SerdeError("key is required", 0);
    }
    return {
      key: this.keySerde.deserialize(key),
      value: this.valueSerde.deserialize(value)
    };
  }
}

export function makeStreamSerde<T>(serde: Serde<T>): StreamSerde<T> {
  return new ValueStreamSerde(serde);
}

export function makeStreamKeyValueSerde<K, V>(
  keySerde: Serde<K>,
  valueSerde: Serde<V>
): StreamSerde<KeyValue<K, V>> {
  return new StreamKeyValueSerde(keySerde, valueSerde);
}
