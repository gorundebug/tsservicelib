import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { Context, MessageContext } from "@gorundebug/tsservicelib/runtime";

/** Test-only observation of real source cancellation joins, without changing signals. */
export function observeCancellationListeners() {
  const signals = new Set<AbortSignal>();
  // The original method is deliberately invoked with call(this, signal) below.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const original = Context.prototype.withExternalCancellation;
  Context.prototype.withExternalCancellation = function (signal: AbortSignal): Context {
    signals.add(this.signal());
    signals.add(signal);
    return original.call(this, signal);
  };
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const originalMessage = MessageContext.prototype.withExternalCancellation;
  MessageContext.prototype.withExternalCancellation = function (signal: AbortSignal): MessageContext {
    signals.add(this.signal());
    signals.add(signal);
    return originalMessage.call(this, signal);
  };
  return {
    restore: () => {
      Context.prototype.withExternalCancellation = original;
      MessageContext.prototype.withExternalCancellation = originalMessage;
    },
    assertBounded: () => {
      assert.ok(signals.size > 0);
      for (const signal of signals)
        assert.ok(getEventListeners(signal, "abort").length <= 4,
          `completed requests retained ${String(getEventListeners(signal, "abort").length)} abort listeners`);
    }
  };
}
