import { type Collector } from "./collector.js";
import type { MessageContext } from "./context.js";
import type { StreamConfig } from "./config/index.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import type { StreamSerde } from "./serde/index.js";
import { ServiceStream, type Completion, type Stream, type TypedStream, type TypedStreamConsumer } from "./stream.js";
export declare class ConsumedStream<T> extends ServiceStream implements TypedStream<T>, Collector<T> {
    #private;
    constructor(config: StreamConfig, environment: RuntimeEnvironment, serde: StreamSerde<T>);
    serde(): StreamSerde<T>;
    typeName(): string;
    consumer(): TypedStreamConsumer<T> | undefined;
    consumers(): readonly Stream[];
    setConsumer(consumer: TypedStreamConsumer<T>): void;
    emit(context: MessageContext, value: T): Completion;
    out(context: MessageContext, value: T): Completion;
    collector(): Collector<T>;
}
//# sourceMappingURL=consumed-stream.d.ts.map