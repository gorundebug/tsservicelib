import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { DelayStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { DelayFunction } from "./functions.js";
export declare class DelayStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: DelayStreamConfig, source: TypedStream<T>, function_: DelayFunction<T>);
    functionImplementation(): DelayFunction<T>;
    consume(context: MessageContext, value: T): Promise<void>;
}
export declare function makeDelayStream<T>(config: DelayStreamConfig, source: TypedStream<T>, function_: DelayFunction<T>): DelayStream<T>;
//# sourceMappingURL=delay.d.ts.map