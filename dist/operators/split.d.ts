import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { SplitStreamConfig } from "../runtime/config/types.js";
import type { MessageContext } from "../runtime/context.js";
import type { RuntimeBuildable } from "../runtime/environment/runtime-environment.js";
import type { Completion, Stream, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
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