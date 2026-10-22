import type { MessageContext } from "./context.js";
import type { Caller } from "./stream.js";
/** Counts a graph link without loading any tracing runtime. */
export declare class CountedCaller<T> implements Caller<T> {
    private readonly caller;
    private readonly recordCall;
    constructor(caller: Caller<T>, recordCall: (context: MessageContext) => void);
    isAsync(): boolean;
    consume(context: MessageContext, value: T): void | Promise<void>;
}
//# sourceMappingURL=counted-caller.d.ts.map