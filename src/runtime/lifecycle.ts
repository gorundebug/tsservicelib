import type { Context } from "./context.js";

export interface Lifecycle {
  start(context: Context): Promise<void>;
  stop(context: Context): Promise<void>;
}

export interface AdmissionLifecycle extends Lifecycle {
  stopAdmission(context: Context): Promise<void>;
}

export type ComponentCategory =
  | "dataSource"
  | "dataSink"
  | "managedDataConnector"
  | "storage"
  | "delayPool"
  | "taskPool"
  | "priorityTaskPool"
  | "component"
  | "httpServer"
  | "telemetry";

export interface RuntimeComponent {
  readonly category: ComponentCategory;
  readonly name: string;
  readonly lifecycle: Lifecycle;
}

/** Observe every operation, but stop waiting when the shared shutdown context expires.
 * An undefined result means the operation still owns its resources and may finish later.
 */
export async function settleWithinDeadline<T>(
  context: Context,
  operations: readonly Promise<T>[]
): Promise<readonly (PromiseSettledResult<T> | undefined)[]> {
  if (operations.length === 0) return [];
  const results = new Array<PromiseSettledResult<T> | undefined>(operations.length);
  const tracked = operations.map(async (operation, index) => {
    try {
      results[index] = { status: "fulfilled", value: await operation };
    } catch (reason: unknown) {
      results[index] = { status: "rejected", reason };
    }
  });
  if (context.cancelled()) return [...results];
  const signal = context.signal();
  const remainingMs = context.remainingMs();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: () => void = () => undefined;
  const cancelled = new Promise<void>((resolve) => {
    onAbort = resolve;
    signal.addEventListener("abort", onAbort, { once: true });
    if (remainingMs !== undefined) {
      const checkDeadline = (): void => {
        const remaining = context.remainingMs();
        if (remaining !== undefined && remaining > 0) {
          // Node truncates fractional delays. Never publish a timeout before
          // the caller's actual deadline, even if the timer wakes up early.
          timer = setTimeout(checkDeadline, Math.max(1, Math.ceil(remaining)));
        } else {
          resolve();
        }
      };
      timer = setTimeout(checkDeadline, Math.max(0, Math.ceil(remainingMs)));
    }
    if (signal.aborted) resolve();
  });
  try {
    await Promise.race([Promise.all(tracked), cancelled]);
    return [...results];
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    signal.removeEventListener("abort", onAbort);
  }
}
