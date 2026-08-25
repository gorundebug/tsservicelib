import { SpanStatusCode, spanError, stringAttribute } from "./environment/tracing/index.js";
export const DurableCallEvent = {
    Heartbeat: "heartbeat",
    Success: "success",
    Error: "error",
    MissingOutcome: "missing_outcome",
    DuplicateTerminal: "duplicate_terminal",
    LateHeartbeat: "late_heartbeat",
    Suspended: "suspended"
};
export class DurableCallContextError extends Error {
}
export class NoDurableCallContextError extends DurableCallContextError {
}
export class DurableCallAlreadyCompletedError extends DurableCallContextError {
}
export class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {
}
export class DurableCallOutcomeMissingError extends DurableCallContextError {
}
export class DurableCallContext {
    parentCallId;
    #occurrences = new Map();
    #terminal = Promise.withResolvers();
    #heartbeat;
    #diagnostics;
    #completed = false;
    #outcome;
    #span;
    #spanEnded = false;
    #delayAtUnixMillis;
    #continuation;
    constructor(parentCallId, heartbeat, diagnostics) {
        this.parentCallId = parentCallId;
        this.#heartbeat = heartbeat;
        this.#diagnostics = diagnostics;
    }
    occurrence(key) {
        const next = (this.#occurrences.get(key) ?? 0) + 1;
        this.#occurrences.set(key, next);
        return next;
    }
    bindSpan(span) {
        this.#span = span;
    }
    heartbeat(message) {
        if (this.#completed) {
            const error = new DurableCallHeartbeatAfterCompletionError("durable call heartbeat after completion");
            this.report(DurableCallEvent.LateHeartbeat, error);
            throw error;
        }
        this.#heartbeat?.(message);
        this.report(DurableCallEvent.Heartbeat);
    }
    success() {
        this.complete(DurableCallEvent.Success);
    }
    fail(error) {
        this.complete(DurableCallEvent.Error, error);
    }
    cancelWithoutOutcome(cause) {
        if (this.#completed)
            return;
        const detail = errorFromUnknown(cause);
        this.complete(DurableCallEvent.MissingOutcome, new DurableCallOutcomeMissingError(`durable call completed without explicit outcome${detail === undefined ? "" : `: ${detail.message}`}`, detail === undefined ? undefined : { cause: detail }));
    }
    async wait() {
        const result = await this.#terminal.promise;
        if (this.#outcome !== undefined)
            throw this.#outcome;
        return result;
    }
    beginDelay(delayMs) {
        if (this.#completed) {
            throw new DurableCallAlreadyCompletedError("durable call is already completed; attempted delay");
        }
        if (this.#delayAtUnixMillis !== undefined) {
            throw new DurableCallContextError("durable delay is already pending");
        }
        this.#delayAtUnixMillis = Date.now() + delayMs;
    }
    captureContinuation(context, fromName, toName, payload) {
        if (this.#delayAtUnixMillis === undefined)
            return false;
        if (this.#completed) {
            throw new DurableCallAlreadyCompletedError("durable call is already completed; attempted suspension");
        }
        const remainingMs = context.remainingMs();
        this.#continuation = {
            version: 1,
            fromName,
            toName,
            callId: `${this.parentCallId}/delay`,
            streamId: context.streamId() ?? "",
            priority: context.priority() ?? 0,
            deadlineUnixMillis: remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
            wakeAtUnixMillis: this.#delayAtUnixMillis,
            traceCarrier: Object.fromEntries(context.transportMetadata()),
            payload: Uint8Array.from(payload)
        };
        this.#completed = true;
        this.report(DurableCallEvent.Suspended);
        this.#terminal.resolve({ continuation: this.#continuation });
        return true;
    }
    finishSpan() {
        if (this.#span === undefined || this.#spanEnded)
            return;
        this.#spanEnded = true;
        if (this.#outcome === undefined)
            this.#span.setStatus(SpanStatusCode.Ok, "");
        this.#span.end();
    }
    complete(event, outcome) {
        if (this.#completed) {
            const error = new DurableCallAlreadyCompletedError(`durable call is already completed; attempted ${event}`);
            this.report(DurableCallEvent.DuplicateTerminal, error);
            throw error;
        }
        this.#completed = true;
        this.#outcome = outcome;
        this.report(event, outcome);
        this.#terminal.resolve({});
    }
    report(event, error) {
        const attributes = [stringAttribute("event", event)];
        if (error !== undefined)
            attributes.push(stringAttribute("error", error.message));
        this.#span?.addEvent(`durable_call.${event}`, attributes);
        if (this.#span !== undefined &&
            (event === DurableCallEvent.Error || event === DurableCallEvent.MissingOutcome)) {
            spanError(this.#span, error ?? new Error(event));
        }
        this.#diagnostics?.(event, error);
    }
}
function requireDurableCallContext(context, operation) {
    const durable = context.durableCallContext();
    if (durable instanceof DurableCallContext)
        return durable;
    const error = new NoDurableCallContextError(`DurableCall ${operation} invoked outside an Activity`);
    process.emitWarning(error.message, { code: "SERVICELIB_DURABLE_CALL_CONTEXT" });
    throw error;
}
export function durableCallHeartbeat(context, message) {
    requireDurableCallContext(context, DurableCallEvent.Heartbeat).heartbeat(message);
}
export function durableCallSuccess(context) {
    requireDurableCallContext(context, DurableCallEvent.Success).success();
}
export function durableCallError(context, error) {
    requireDurableCallContext(context, DurableCallEvent.Error).fail(error);
}
export function beginDurableDelay(context, delayMs) {
    const durable = context.durableCallContext();
    if (!(durable instanceof DurableCallContext))
        return false;
    durable.beginDelay(delayMs);
    return true;
}
export function captureDurableContinuation(context, fromName, toName, payload) {
    const durable = context.durableCallContext();
    if (!(durable instanceof DurableCallContext))
        return false;
    return durable.captureContinuation(context, fromName, toName, payload);
}
export function bindDurableCallSpan(context, span) {
    const durable = context.durableCallContext();
    if (!(durable instanceof DurableCallContext))
        return false;
    durable.bindSpan(span);
    return true;
}
export async function runDurableCallActivity(signal, durable, invoke) {
    const cancelled = () => {
        durable.cancelWithoutOutcome(signal.reason);
    };
    signal.addEventListener("abort", cancelled, { once: true });
    if (signal.aborted)
        cancelled();
    try {
        try {
            await invoke();
        }
        catch (error) {
            if (signal.aborted)
                durable.cancelWithoutOutcome(error);
            else {
                try {
                    durable.fail(errorFromUnknown(error) ?? new Error("unknown DurableCall failure"));
                }
                catch (terminalError) {
                    if (!(terminalError instanceof DurableCallAlreadyCompletedError))
                        throw terminalError;
                }
            }
        }
        return await durable.wait();
    }
    finally {
        signal.removeEventListener("abort", cancelled);
        durable.finishSpan();
    }
}
function errorFromUnknown(value) {
    if (value === undefined)
        return undefined;
    if (value instanceof Error)
        return value;
    if (typeof value === "string")
        return new Error(value);
    return new Error("non-Error DurableCall failure");
}
//# sourceMappingURL=durable-call-context.js.map