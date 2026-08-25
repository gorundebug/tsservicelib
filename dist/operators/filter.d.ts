import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { FilterStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { FilterFunction } from "./functions.js";
export declare class FilterStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: FilterStreamConfig, source: TypedStream<T>, function_: FilterFunction<T>);
    source(): TypedStream<T>;
    functionImplementation(): FilterFunction<T>;
    consume(context: MessageContext, value: T): Promise<void>;
    private consumeFiltered;
}
export declare function makeFilterStream<T>(config: FilterStreamConfig, source: TypedStream<T>, function_: FilterFunction<T>): FilterStream<T>;
//# sourceMappingURL=filter.d.ts.map