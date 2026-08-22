export class SerdeError extends Error {
    offset;
    constructor(message, offset) {
        super(`${message} at byte ${String(offset)}`);
        this.offset = offset;
        this.name = "SerdeError";
    }
}
export const unlimitedSerdeLimits = Object.freeze({
    maxStringBytes: Number.MAX_SAFE_INTEGER,
    maxBytes: Number.MAX_SAFE_INTEGER,
    maxContainerElements: Number.MAX_SAFE_INTEGER,
    maxTotalBytes: Number.MAX_SAFE_INTEGER
});
export class ValueSerde {
    isStub() {
        return false;
    }
}
export class StubSerde {
    serialize(value, prefix) {
        void value;
        void prefix;
        throw new SerdeError("stub serde cannot serialize", 0);
    }
    deserialize(data) {
        void data;
        throw new SerdeError("stub serde cannot deserialize", 0);
    }
    isStub() {
        return true;
    }
}
//# sourceMappingURL=serde.js.map