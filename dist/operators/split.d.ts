import { ConsumedStream, type Completion, type MessageContext, type RuntimeBuildable, type SplitStreamConfig, type Stream, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
export declare class SplitStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T>, RuntimeBuildable {
    #private;
    constructor(config: SplitStreamConfig, source: TypedStream<T>);
    addStream(): TypedStream<T>;
    build(): void;
    consumers(): readonly Stream[];
    consume(context: MessageContext, value: T): Completion;
    private emitLinks;
    functionImplementation(): undefined;
}
export declare function makeSplitStream<T>(config: SplitStreamConfig, source: TypedStream<T>): SplitStream<T>;
//# sourceMappingURL=split.d.ts.map