import { SpanStatusCode, spanError, stringAttribute } from "./environment/tracing/index.js";
export const DurableCallEvent = {
    Heartbeat: "heartbeat",
    Success: "success",
    Error: "error",
    LateHeartbeat: "late_heartbeat"
};
export class DurableCallContextError extends Error {
}
export class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {
}
/** Processing-side state for one Temporal endpoint Activity. */
export class DurableCallContext {
    messageId;
    #heartbeat;
    #diagnostics;
    #closed = false;
    #span;
    #spanEnded = false;
    constructor(messageId, heartbeat, diagnostics) {
        this.messageId = messageId;
        this.#heartbeat = heartbeat;
        this.#diagnostics = diagnostics;
    }
    bindSpan(span) {
        this.#span = span;
    }
    heartbeat(message) {
        if (this.#closed) {
            const error = new DurableCallHeartbeatAfterCompletionError("durable call heartbeat after completion");
            this.report(DurableCallEvent.LateHeartbeat, error);
            throw error;
        }
        this.#heartbeat?.(message);
        this.report(DurableCallEvent.Heartbeat);
    }
    close(error) {
        if (this.#closed)
            return;
        this.#closed = true;
        this.report(error === undefined ? DurableCallEvent.Success : DurableCallEvent.Error, error);
        if (this.#span !== undefined && !this.#spanEnded) {
            this.#spanEnded = true;
            if (error === undefined)
                this.#span.setStatus(SpanStatusCode.Ok, "");
            this.#span.end();
        }
    }
    report(event, error) {
        const attributes = [stringAttribute("event", event)];
        if (error !== undefined)
            attributes.push(stringAttribute("error", error.message));
        this.#span?.addEvent(`temporal.activity.${event}`, attributes);
        if (this.#span !== undefined && event === DurableCallEvent.Error) {
            spanError(this.#span, error ?? new Error(event));
        }
        this.#diagnostics?.(event, error);
    }
}
export function durableCallHeartbeat(context, message) {
    const durable = context.durableCallContext();
    if (durable instanceof DurableCallContext)
        durable.heartbeat(message);
}
export function bindDurableCallSpan(context, span) {
    const durable = context.durableCallContext();
    if (!(durable instanceof DurableCallContext))
        return false;
    durable.bindSpan(span);
    return true;
}
export async function runDurableCallActivity(durable, invoke) {
    try {
        const result = await invoke();
        durable.close();
        return result;
    }
    catch (error) {
        const failure = errorFromUnknown(error);
        durable.close(failure);
        throw failure;
    }
}
function errorFromUnknown(value) {
    if (value instanceof Error)
        return value;
    if (typeof value === "string")
        return new Error(value);
    return new Error("non-Error Temporal Activity failure");
}
//# sourceMappingURL=durable-call-context.js.map