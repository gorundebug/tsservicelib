import type { MessageContext } from "./context.js";
import type { Caller, Completion } from "./stream.js";
export interface Collector<T> {
    out(context: MessageContext, value: T): Completion;
}
export type CollectFunction<T> = (context: MessageContext, value: T) => Completion;
export declare class FunctionCollector<T> implements Collector<T> {
    #private;
    constructor(collect: CollectFunction<T>);
    out(context: MessageContext, value: T): Completion;
}
export declare class CallerCollector<T> implements Collector<T> {
    #private;
    constructor(caller?: Caller<T>);
    out(context: MessageContext, value: T): Completion;
}
export declare function makeCollector<T>(caller?: Caller<T>): Collector<T>;
//# sourceMappingURL=collector.d.ts.map