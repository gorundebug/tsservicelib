import type { TemporalExecutionType } from "../../runtime/config/types.js";
import type { MessageContext } from "../../runtime/context.js";
/** Must be deterministic when attached to a Workflow graph. */
export type SubmissionOptionsProvider<T> = (context: MessageContext, value: T) => SubmissionOptions;
/** Explicit per-call transport options; placement and failover remain application-owned. */
export interface SubmissionOptions {
    /** Empty/absent retains the configured queue. */
    readonly taskQueue?: string;
    /** Milliseconds waiting for an Activity worker, not its execution timeout. */
    readonly scheduleToStartTimeout?: number;
}
export declare function resolveSubmissionQueue(configured: string, execution: TemporalExecutionType, options: SubmissionOptions): string;
//# sourceMappingURL=submission-options.d.ts.map