import { appendBytes, encodeSize, SerdeReader } from "./framing.js";
import { SerdeError } from "./serde.js";
class ValueStreamSerde {
    valueSerde;
    constructor(valueSerde) {
        this.valueSerde = valueSerde;
    }
    serialize(value, prefix) {
        return this.valueSerde.serialize(value, prefix);
    }
    deserialize(data) {
        return this.valueSerde.deserialize(data);
    }
    isStub() {
        return this.valueSerde.isStub();
    }
    typeName() {
        return "";
    }
    isKeyValue() {
        return false;
    }
    serializeKey(value) {
        void value;
        return undefined;
    }
    serializeValue(value) {
        return this.valueSerde.serialize(value);
    }
    deserializeKeyValue(_key, value) {
        return this.valueSerde.deserialize(value);
    }
}
export class StreamKeyValueSerde {
    keySerde;
    valueSerde;
    constructor(keySerde, valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }
    serialize(value, prefix) {
        const key = this.keySerde.serialize(value.key);
        const encodedValue = this.valueSerde.serialize(value.value);
        let output = appendBytes(prefix, encodeSize(key.byteLength));
        output = appendBytes(output, key);
        output = appendBytes(output, encodeSize(encodedValue.byteLength));
        return appendBytes(output, encodedValue);
    }
    deserialize(data) {
        const reader = new SerdeReader(data, Number.MAX_SAFE_INTEGER);
        const keyLength = reader.readSize(data.byteLength, "key length");
        const key = this.keySerde.deserialize(reader.read(keyLength, "key"));
        const valueLength = reader.readSize(data.byteLength, "value length");
        const value = this.valueSerde.deserialize(reader.read(valueLength, "value"));
        return { key, value };
    }
    isStub() {
        return this.keySerde.isStub() || this.valueSerde.isStub();
    }
    typeName() {
        return "";
    }
    isKeyValue() {
        return true;
    }
    serializeKey(value) {
        return this.keySerde.serialize(value.key);
    }
    serializeValue(value) {
        return this.valueSerde.serialize(value.value);
    }
    deserializeKeyValue(key, value) {
        if (key === undefined) {
            throw new SerdeError("key is required", 0);
        }
        return {
            key: this.keySerde.deserialize(key),
            value: this.valueSerde.deserialize(value)
        };
    }
}
export function makeStreamSerde(serde) {
    return new ValueStreamSerde(serde);
}
export function makeStreamKeyValueSerde(keySerde, valueSerde) {
    return new StreamKeyValueSerde(keySerde, valueSerde);
}
//# sourceMappingURL=stream.js.map