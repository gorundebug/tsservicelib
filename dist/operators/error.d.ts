import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { StreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import type { Completion, Stream, TypedStreamConsumer } from "../runtime/stream.js";
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