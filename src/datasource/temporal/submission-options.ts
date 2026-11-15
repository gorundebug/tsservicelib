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

export function resolveSubmissionQueue(
  configured: string,
  execution: TemporalExecutionType,
  options: SubmissionOptions
): string {
  const queue = options.taskQueue ?? "";
  if (queue !== "" && (queue.trim() === "" || queue.includes("\0"))) {
    throw new Error("Temporal submission task queue is invalid");
  }
  const timeout = options.scheduleToStartTimeout ?? 0;
  if (!Number.isFinite(timeout) || timeout < 0) {
    throw new Error(
      "Temporal submission schedule-to-start timeout must not be negative or non-finite"
    );
  }
  if (timeout !== 0 && execution !== "Activity") {
    throw new Error("Temporal submission schedule-to-start timeout requires an Activity endpoint");
  }
  return queue || configured;
}
