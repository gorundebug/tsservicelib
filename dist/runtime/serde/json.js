import { appendBytes, validateLimits } from "./framing.js";
import { SerdeError, unlimitedSerdeLimits, ValueSerde } from "./serde.js";
export class JsonSerde extends ValueSerde {
    type;
    #encoder = new TextEncoder();
    #decoder = new TextDecoder("utf-8", { fatal: true });
    #limits;
    constructor(type, limits = unlimitedSerdeLimits) {
        super();
        this.type = type;
        validateLimits(limits);
        this.#limits = limits;
    }
    serialize(value, prefix) {
        this.type.assert(value);
        let json;
        try {
            json = stringify(value);
        }
        catch (error) {
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
    deserialize(data) {
        if (data.byteLength > this.#limits.maxTotalBytes || data.byteLength > this.#limits.maxBytes) {
            throw new SerdeError("JSON input exceeds configured limit", 0);
        }
        let json;
        try {
            json = this.#decoder.decode(data);
        }
        catch (error) {
            throw new SerdeError(jsonErrorMessage("JSON input is not valid UTF-8", error), 0);
        }
        let value;
        try {
            value = JSON.parse(json);
        }
        catch (error) {
            throw new SerdeError(jsonErrorMessage("JSON parsing failed", error), jsonErrorOffset(error));
        }
        this.type.assert(value);
        return value;
    }
}
function stringify(value) {
    return JSON.stringify(value);
}
function jsonErrorMessage(prefix, error) {
    return `${prefix}${error instanceof Error ? `: ${error.message}` : ""}`;
}
function jsonErrorOffset(error) {
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
//# sourceMappingURL=json.js.map