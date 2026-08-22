import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { appendBytes } from "./framing.js";
import { SerdeError, ValueSerde } from "./serde.js";
export class ProtobufSerde extends ValueSerde {
    schema;
    maxBytes;
    constructor(schema, maxBytes = Number.MAX_SAFE_INTEGER) {
        super();
        this.schema = schema;
        this.maxBytes = maxBytes;
        if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
            throw new RangeError("maxBytes must be a non-negative safe integer");
        }
    }
    serialize(value, prefix) {
        let encoded;
        try {
            encoded = toBinary(this.schema, value);
        }
        catch (error) {
            throw new SerdeError(protobufErrorMessage("protobuf serialization failed", error), 0);
        }
        if (encoded.byteLength > this.maxBytes) {
            throw new SerdeError("protobuf output exceeds configured limit", 0);
        }
        return appendBytes(prefix, encoded);
    }
    deserialize(data) {
        if (data.byteLength > this.maxBytes) {
            throw new SerdeError("protobuf input exceeds configured limit", 0);
        }
        try {
            return fromBinary(this.schema, data);
        }
        catch (error) {
            throw new SerdeError(protobufErrorMessage("protobuf deserialization failed", error), 0);
        }
    }
}
function protobufErrorMessage(prefix, error) {
    return `${prefix}${error instanceof Error ? `: ${error.message}` : ""}`;
}
//# sourceMappingURL=protobuf.js.map