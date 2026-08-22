import { ConsumedStream, type Completion, type MessageContext, type StreamConfig, type Stream, type RuntimeEnvironment, type StreamSerde, type TypedStreamConsumer } from "../runtime/index.js";
export declare class ErrorStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: StreamConfig, environment: RuntimeEnvironment, serde: StreamSerde<T>, owner: Stream);
    /** Mirrors Go ErrorStream.GetID without changing the configured stream ID. */
    get id(): number;
    consume(context: MessageContext, value: T): Completion;
    out(context: MessageContext, value: T): Completion;
    functionImplementation(): undefined;
}
//# sourceMappingURL=error.d.ts.map