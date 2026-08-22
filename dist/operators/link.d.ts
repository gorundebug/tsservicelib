import { ConsumedStream, type Completion, type CycleLinkStreamConfig, type MessageContext, type RuntimeEnvironment, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
/** A graph root whose source is connected after the acyclic graph is built. */
export declare class LinkStream<T> extends ConsumedStream<T> implements TypedStreamConsumer<T> {
    #private;
    constructor(config: CycleLinkStreamConfig, environment: RuntimeEnvironment);
    source(): TypedStream<T> | undefined;
    setSource(source: TypedStream<T>): void;
    consume(context: MessageContext, value: T): Completion;
    functionImplementation(): undefined;
}
export declare function makeLinkStream<T>(config: CycleLinkStreamConfig, environment: RuntimeEnvironment): LinkStream<T>;
//# sourceMappingURL=link.d.ts.map