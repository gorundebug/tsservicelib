import type { MessageContext } from "./context.js";
import type { StreamConfig } from "./config/index.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import { type StartedSpan } from "./environment/index.js";
import type { StreamSerde } from "./serde/index.js";
export type Completion = void | Promise<void>;
export interface Stream {
    readonly id: number;
    readonly name: string;
    readonly transformationName: string;
    runtimeEnvironment(): RuntimeEnvironment;
    config(): StreamConfig;
}
export interface Consumer<T> {
    consume(context: MessageContext, value: T): Completion;
}
export interface Caller<T> extends Consumer<T> {
    isAsync(): boolean;
}
export interface TypedStreamConsumer<T> extends Stream, Consumer<T> {
}
export interface TypedConsumedStream<T> extends TypedStream<T>, TypedStreamConsumer<T> {
}
export interface TypedStream<T> extends Stream {
    serde(): StreamSerde<T>;
    typeName(): string;
    consumer(): TypedStreamConsumer<T> | undefined;
    consumers(): readonly Stream[];
    setConsumer(consumer: TypedStreamConsumer<T>): void;
}
export interface CallerFactory {
    create<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T>;
}
/**
 * Direct delivery preserves FunctionCall semantics. The async bit is graph
 * metadata and never turns this call into detached work.
 */
export declare class FunctionCaller<T> implements Caller<T> {
    #private;
    constructor(consumer: Consumer<T>, async?: boolean);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): Completion;
}
/** Stores only immutable graph identity; reloadable config is resolved elsewhere by ID. */
export declare class ServiceStream implements Stream {
    #private;
    readonly transformationName: string;
    constructor(config: StreamConfig, environment: RuntimeEnvironment);
    get id(): number;
    get name(): string;
    runtimeEnvironment(): RuntimeEnvironment;
    config(): StreamConfig;
    protected tracingEnabled(context: MessageContext): boolean;
    protected startSpan(context: MessageContext, operation: string): StartedSpan | undefined;
    protected traceCompletion(context: MessageContext, operation: string, consume: (spanContext: MessageContext) => Completion): Completion;
}
//# sourceMappingURL=stream.d.ts.map