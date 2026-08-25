import type { Collector } from "../runtime/collector.js";
import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { MapStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
import type { MapFunction } from "./functions.js";
export declare class MapStream<T, R> extends ConsumedStream<R> implements TypedStreamConsumer<T>, Collector<R> {
    #private;
    constructor(config: MapStreamConfig, source: TypedStream<T>, function_: MapFunction<T, R>);
    source(): TypedStream<T>;
    functionImplementation(): MapFunction<T, R>;
    consume(context: MessageContext, value: T): Completion;
}
export declare function makeMapStream<T, R>(config: MapStreamConfig, source: TypedStream<T>, function_: MapFunction<T, R>): MapStream<T, R>;
//# sourceMappingURL=map.d.ts.map