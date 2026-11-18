import { type Context } from "../../runtime/index.js";
export declare function cancellation(context: Context): Error;
/** Observes late rejection, removes listeners, and never interprets cancellation as withdrawal. */
export declare function wait<T>(operation: Promise<T>, context: Context, timeoutMs?: number): Promise<T>;
//# sourceMappingURL=wait.d.ts.map