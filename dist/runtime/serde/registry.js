const registerSerde = Symbol("registerSerde");
const resolveSerde = Symbol("resolveSerde");
export class SerdeType {
    name;
    predicate;
    #registered = new WeakMap();
    constructor(name, predicate) {
        this.name = name;
        this.predicate = predicate;
        if (name.trim().length === 0) {
            throw new Error("serde type name must not be empty");
        }
    }
    is(value) {
        return this.predicate(value);
    }
    assert(value) {
        if (!this.predicate(value)) {
            throw new TypeError(`value is not ${this.name}`);
        }
    }
    [registerSerde](registry, serde) {
        this.#registered.set(registry, serde);
    }
    [resolveSerde](registry) {
        return this.#registered.get(registry);
    }
}
export class SerdeRegistry {
    #byName = new Map();
    #assertByName = new Map();
    #streamValueTypes = new Map();
    #streamErrorTypes = new Map();
    register(type, serde) {
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
    get(type) {
        return type[resolveSerde](this);
    }
    require(type) {
        const serde = this.get(type);
        if (serde === undefined) {
            throw new Error(`serde type ${type.name} is not registered`);
        }
        return serde;
    }
    /** Resolve graph type metadata after TypeScript generic types have been erased. */
    requireByName(name) {
        const serde = this.#byName.get(name);
        if (serde === undefined) {
            throw new Error(`serde type ${name} is not registered`);
        }
        // Every registry entry is guarded by RuntimeValidatedStreamSerde. The cast is
        // confined to this runtime boundary; invalid values still fail validation.
        return serde;
    }
    matchesByName(name, value) {
        const assert = this.#assertByName.get(name);
        if (assert === undefined) {
            throw new Error(`serde type ${name} is not registered`);
        }
        try {
            assert(value);
            return true;
        }
        catch (error) {
            if (error instanceof TypeError)
                return false;
            throw error;
        }
    }
    // The type parameter reifies graph metadata at the single erased runtime boundary.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
    assertByName(name, value) {
        const assert = this.#assertByName.get(name);
        if (assert === undefined) {
            throw new Error(`serde type ${name} is not registered`);
        }
        assert(value);
    }
    registerStreamErrorType(streamId, type) {
        if (this.#streamErrorTypes.has(streamId)) {
            throw new Error(`error serde for stream ${String(streamId)} is already registered`);
        }
        this.#streamErrorTypes.set(streamId, type.name);
    }
    /** Registers generated graph type metadata lost to JavaScript type erasure. */
    registerStreamValueType(streamId, type) {
        if (this.#streamValueTypes.has(streamId)) {
            throw new Error(`value serde for stream ${String(streamId)} is already registered`);
        }
        this.#streamValueTypes.set(streamId, type.name);
    }
    requireStreamValue(streamId) {
        const name = this.#streamValueTypes.get(streamId);
        if (name === undefined) {
            throw new Error(`value serde for stream ${String(streamId)} is not registered`);
        }
        return this.requireByName(name);
    }
    requireStreamError(streamId) {
        const name = this.#streamErrorTypes.get(streamId);
        if (name === undefined) {
            throw new Error(`error serde for stream ${String(streamId)} is not registered`);
        }
        return this.requireByName(name);
    }
}
class RuntimeValidatedStreamSerde {
    type;
    serde;
    constructor(type, serde) {
        this.type = type;
        this.serde = serde;
    }
    serialize(value, prefix) {
        this.type.assert(value);
        return this.serde.serialize(value, prefix);
    }
    deserialize(data) {
        return this.validate(this.serde.deserialize(data));
    }
    isStub() {
        return this.serde.isStub();
    }
    typeName() {
        return this.type.name;
    }
    isKeyValue() {
        return this.serde.isKeyValue();
    }
    serializeKey(value) {
        this.type.assert(value);
        return this.serde.serializeKey(value);
    }
    serializeValue(value) {
        this.type.assert(value);
        return this.serde.serializeValue(value);
    }
    deserializeKeyValue(key, value) {
        return this.validate(this.serde.deserializeKeyValue(key, value));
    }
    validate(value) {
        this.type.assert(value);
        return value;
    }
}
//# sourceMappingURL=registry.js.map