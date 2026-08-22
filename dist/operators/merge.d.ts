import { ConsumedStream, type Completion, type MergeStreamConfig, type MessageContext, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
export declare class MergeStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    constructor(config: MergeStreamConfig, sources: readonly [TypedStream<T>, ...TypedStream<T>[]]);
    consume(context: MessageContext, value: T): Completion;
    functionImplementation(): undefined;
}
export declare function makeMergeStream<T>(config: MergeStreamConfig, source: TypedStream<T>, ...sources: TypedStream<T>[]): MergeStream<T>;
//# sourceMappingURL=merge.d.ts.map