import { fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";

import { appendBytes } from "./framing.js";
import { SerdeError, ValueSerde } from "./serde.js";

export class ProtobufSerde<Desc extends DescMessage> extends ValueSerde<MessageShape<Desc>> {
  public constructor(
    private readonly schema: Desc,
    private readonly maxBytes = Number.MAX_SAFE_INTEGER
  ) {
    super();
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
      throw new RangeError("maxBytes must be a non-negative safe integer");
    }
  }

  public serialize(value: MessageShape<Desc>, prefix?: Uint8Array): Uint8Array {
    let encoded: Uint8Array;
    try {
      encoded = toBinary(this.schema, value);
    } catch (error: unknown) {
      throw new SerdeError(protobufErrorMessage("protobuf serialization failed", error), 0);
    }
    if (encoded.byteLength > this.maxBytes) {
      throw new SerdeError("protobuf output exceeds configured limit", 0);
    }
    return appendBytes(prefix, encoded);
  }

  public deserialize(data: Uint8Array): MessageShape<Desc> {
    if (data.byteLength > this.maxBytes) {
      throw new SerdeError("protobuf input exceeds configured limit", 0);
    }
    try {
      return fromBinary(this.schema, data);
    } catch (error: unknown) {
      throw new SerdeError(protobufErrorMessage("protobuf deserialization failed", error), 0);
    }
  }
}

function protobufErrorMessage(prefix: string, error: unknown): string {
  return `${prefix}${error instanceof Error ? `: ${error.message}` : ""}`;
}
