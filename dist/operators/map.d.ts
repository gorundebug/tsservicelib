import { ConsumedStream, type Collector, type Completion, type MessageContext, type MapStreamConfig, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
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