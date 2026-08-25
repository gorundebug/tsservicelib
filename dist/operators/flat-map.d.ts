import type { Collector } from "../runtime/collector.js";
import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { FlatMapStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { FlatMapFunction } from "./functions.js";
export declare class FlatMapStream<T, R> extends ConsumedStream<R> implements TypedStreamConsumer<T>, Collector<R> {
    #private;
    constructor(config: FlatMapStreamConfig, source: TypedStream<T>, function_: FlatMapFunction<T, R>);
    source(): TypedStream<T>;
    functionImplementation(): FlatMapFunction<T, R>;
    consume(context: MessageContext, value: T): Completion;
}
export declare function makeFlatMapStream<T, R>(config: FlatMapStreamConfig, source: TypedStream<T>, function_: FlatMapFunction<T, R>): FlatMapStream<T, R>;
//# sourceMappingURL=flat-map.d.ts.map