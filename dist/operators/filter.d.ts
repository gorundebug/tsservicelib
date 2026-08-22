import { ConsumedStream, type MessageContext, type FilterStreamConfig, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
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