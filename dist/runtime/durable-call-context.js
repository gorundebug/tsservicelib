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
    executionType;
    #heartbeat;
    #timer;
    #diagnostics;
    #closed = false;
    #span;
    #spanEnded = false;
    constructor(messageId, executionType, options = {}) {
        this.messageId = messageId;
        this.executionType = executionType;
        this.#heartbeat = options.heartbeat;
        this.#timer = options.timer;
        this.#diagnostics = options.diagnostics;
        if (executionType === "Activity" && this.#timer !== undefined) {
            throw new Error("Temporal Activity durable context cannot own a Workflow timer");
        }
        if (executionType === "Workflow" && this.#heartbeat !== undefined) {
            throw new Error("Temporal Workflow durable context cannot own an Activity heartbeat");
        }
    }
    bindSpan(span) {
        this.#span = span;
    }
    heartbeat(message) {
        if (this.executionType !== "Activity")
            return;
        if (this.#closed) {
            const error = new DurableCallHeartbeatAfterCompletionError("durable call heartbeat after completion");
            this.report(DurableCallEvent.LateHeartbeat, error);
            throw error;
        }
        this.#heartbeat?.(message);
        this.report(DurableCallEvent.Heartbeat);
    }
    async delay(delayMs) {
        if (this.executionType !== "Workflow")
            return false;
        if (this.#closed)
            throw new DurableCallContextError("durable call timer after completion");
        if (this.#timer === undefined) {
            throw new DurableCallContextError("Temporal Workflow durable timer is not configured");
        }
        await this.#timer(delayMs);
        return true;
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
/** Returns true when a Temporal Workflow timer handled the delay. */
export async function durableCallDelay(context, delayMs) {
    const durable = context.durableCallContext();
    return durable instanceof DurableCallContext ? durable.delay(delayMs) : false;
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
export async function runDurableCallWorkflow(durable, invoke) {
    if (durable.executionType !== "Workflow") {
        throw new DurableCallContextError("runDurableCallWorkflow requires a Workflow context");
    }
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