import { type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { ValueSerde } from "./serde.js";
export declare class ProtobufSerde<Desc extends DescMessage> extends ValueSerde<MessageShape<Desc>> {
    private readonly schema;
    private readonly maxBytes;
    constructor(schema: Desc, maxBytes?: number);
    serialize(value: MessageShape<Desc>, prefix?: Uint8Array): Uint8Array;
    deserialize(data: Uint8Array): MessageShape<Desc>;
}
//# sourceMappingURL=protobuf.d.ts.map