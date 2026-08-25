import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { FlatMapIterableStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
/** Arrays and typed arrays have the indexed semantics supported by Go. */
export interface IndexedIterable<T> extends Iterable<T> {
    readonly length: number;
    readonly [index: number]: T;
}
export type FlatMapIterableInput<T> = IndexedIterable<T> | string;
export declare class FlatMapIterableStream<T extends FlatMapIterableInput<R>, R> extends ConsumedStream<R> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: FlatMapIterableStreamConfig, source: TypedStream<T>);
    source(): TypedStream<T>;
    functionImplementation(): undefined;
    consume(context: MessageContext, value: T): Promise<void>;
    private emitItems;
    private emitIndexed;
}
export declare function makeFlatMapIterableStream(config: FlatMapIterableStreamConfig & {
    readonly valueType: "int32" | "uint8";
}, source: TypedStream<string>): FlatMapIterableStream<string, number>;
export declare function makeFlatMapIterableStream<R>(config: FlatMapIterableStreamConfig, source: TypedStream<IndexedIterable<R>>): FlatMapIterableStream<IndexedIterable<R>, R>;
//# sourceMappingURL=flat-map-iterable.d.ts.map