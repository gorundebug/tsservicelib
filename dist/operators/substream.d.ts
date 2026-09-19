import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { SubStreamConfig } from "../runtime/config/types.js";
import { type MessageContext } from "../runtime/context.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import type { SubStream as CallableSubStream, SubStreamCollector, TypedStream, TypedStreamConsumer } from "../runtime/stream.js";
/** A service-local callable entry; source is the ordinary result-producing stream. */
export declare class SubStream<T, R> extends ConsumedStream<T> implements CallableSubStream<T, R> {
    #private;
    constructor(config: SubStreamConfig, environment: RuntimeEnvironment, serde: StreamSerde<T>);
    setConsumer(consumer: TypedStreamConsumer<T>): void;
    setSource(source: TypedStream<R>): void;
    build(): void;
    consume(context: MessageContext, value: T, collector: SubStreamCollector<R>): Promise<void>;
    private dispatch;
}
export declare function makeSubStream<T, R>(config: SubStreamConfig, environment: RuntimeEnvironment): SubStream<T, R>;
//# sourceMappingURL=substream.d.ts.map