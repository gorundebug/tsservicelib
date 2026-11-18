import { errorFromUnknown, type Context } from "../../runtime/index.js";
export function cancellation(context: Context): Error {
  return context.signal().reason === undefined
    ? new Error("Pulsar operation cancelled")
    : errorFromUnknown(context.signal().reason);
}

/** Observes late rejection, removes listeners, and never interprets cancellation as withdrawal. */
export function wait<T>(operation: Promise<T>, context: Context, timeoutMs?: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const finish = (error: Error | undefined, value?: T): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      context.signal().removeEventListener("abort", abort);
      if (error !== undefined) reject(error);
      else resolve(value as T);
    };
    const abort = (): void => {
      finish(cancellation(context));
    };
    const remaining = Math.min(timeoutMs ?? Infinity, context.remainingMs() ?? Infinity);
    if (Number.isFinite(remaining)) {
      const deadline = performance.now() + remaining;
      const tick = (): void => {
        const left = deadline - performance.now();
        if (left <= 0)
          finish(new Error("Pulsar operation deadline exceeded; delivery may be unknown"));
        else timer = setTimeout(tick, Math.min(2147483647, Math.ceil(left)));
      };
      timer = setTimeout(tick, Math.min(2147483647, Math.max(0, Math.ceil(remaining))));
    }
    context.signal().addEventListener("abort", abort, { once: true });
    operation.then(
      (value) => {
        finish(undefined, value);
      },
      (error: unknown) => {
        finish(errorFromUnknown(error));
      }
    );
    if (context.cancelled()) abort();
  });
}
