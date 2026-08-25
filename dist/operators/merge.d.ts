import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { MergeStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { Completion, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
export declare class MergeStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    constructor(config: MergeStreamConfig, sources: readonly [TypedStream<T>, ...TypedStream<T>[]]);
    consume(context: MessageContext, value: T): Completion;
    functionImplementation(): undefined;
}
export declare function makeMergeStream<T>(config: MergeStreamConfig, source: TypedStream<T>, ...sources: TypedStream<T>[]): MergeStream<T>;
//# sourceMappingURL=merge.d.ts.map