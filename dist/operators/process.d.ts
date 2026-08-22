import { ConsumedStream, type Collector, type Completion, type MessageContext, type ProcessStreamConfig, type TypedStream, type TypedStreamConsumer } from "../runtime/index.js";
import { ErrorStream } from "./error.js";
import type { ProcessFunction } from "./functions.js";
export declare class ProcessStream<T, R, E> extends ConsumedStream<R> implements TypedStreamConsumer<T>, Collector<R> {
    #private;
    constructor(config: ProcessStreamConfig, source: TypedStream<T>, function_: ProcessFunction<T, R, E>);
    source(): TypedStream<T>;
    errorStream(): ErrorStream<E>;
    functionImplementation(): ProcessFunction<T, R, E>;
    consume(context: MessageContext, value: T): Completion;
}
export declare function makeProcessStream<T, R, E>(config: ProcessStreamConfig, source: TypedStream<T>, function_: ProcessFunction<T, R, E>): ProcessStream<T, R, E>;
//# sourceMappingURL=process.d.ts.map