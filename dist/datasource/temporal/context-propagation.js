import { AsyncLocalStorage } from "node:async_hooks";
import {} from "@temporalio/activity";
import { defaultPayloadConverter } from "@temporalio/common";
import { MessageContext } from "../../runtime/context.js";
export const TEMPORAL_HEADER_DEADLINE_UNIX_NANO = "servicelib-deadline-unix-nano";
export const TEMPORAL_HEADER_PRIORITY = "servicelib-priority";
const CARRIER_NAMES = ["traceparent", "tracestate", "baggage", "x-trace", "x-stream-id"];
const submissionContext = new AsyncLocalStorage();
const activityMessageContext = new AsyncLocalStorage();
export function runWithTemporalSubmissionContext(context, operation) {
    return submissionContext.run(context, operation);
}
export function currentTemporalActivityMessageContext() {
    const context = activityMessageContext.getStore();
    if (context === undefined) {
        throw new Error("Temporal Activity MessageContext is not initialized");
    }
    return context;
}
export const temporalWorkflowClientInterceptor = {
    async startWithDetails(input, next) {
        const context = submissionContext.getStore();
        if (context === undefined)
            return next(input);
        return next({ ...input, headers: { ...input.headers, ...encodeContext(context) } });
    }
};
export function temporalActivityInterceptors(temporalContext) {
    const inbound = {
        async execute(input, next) {
            let context = decodeContext(input.headers).withExternalCancellation(temporalContext.cancellationSignal);
            if (temporalContext.info.startToCloseTimeoutMs > 0) {
                context = context.bounded(temporalContext.info.startToCloseTimeoutMs);
            }
            return activityMessageContext.run(context, () => next(input));
        }
    };
    return { inbound };
}
function encodeContext(context) {
    const values = new Map(context.transportMetadata());
    const priority = context.priority();
    if (priority !== undefined)
        values.set(TEMPORAL_HEADER_PRIORITY, String(priority));
    const remainingMs = context.remainingMs();
    if (remainingMs !== undefined) {
        const unixMillis = BigInt(Date.now() + Math.max(0, Math.ceil(remainingMs)));
        values.set(TEMPORAL_HEADER_DEADLINE_UNIX_NANO, String(unixMillis * 1000000n));
    }
    return Object.fromEntries([...values].map(([name, value]) => [name, defaultPayloadConverter.toPayload(value)]));
}
function decodeContext(headers) {
    const metadata = new Map();
    for (const name of CARRIER_NAMES) {
        const value = decodeString(headers[name]);
        if (value !== undefined && value !== "")
            metadata.set(name, value);
    }
    let context = new MessageContext().withMetadata(metadata);
    const priority = Number.parseInt(decodeString(headers[TEMPORAL_HEADER_PRIORITY]) ?? "", 10);
    if (Number.isSafeInteger(priority))
        context = context.withPriority(priority);
    const deadline = decodeString(headers[TEMPORAL_HEADER_DEADLINE_UNIX_NANO]);
    if (deadline !== undefined) {
        try {
            const deadlineUnixMillis = Number(BigInt(deadline) / 1000000n);
            if (Number.isSafeInteger(deadlineUnixMillis)) {
                context = context.bounded(Math.max(0, deadlineUnixMillis - Date.now()));
            }
        }
        catch {
            // Invalid external carrier fields are ignored consistently with HTTP/Kafka metadata.
        }
    }
    return context;
}
function decodeString(payload) {
    if (payload === undefined)
        return undefined;
    try {
        const value = defaultPayloadConverter.fromPayload(payload);
        return typeof value === "string" ? value : undefined;
    }
    catch {
        return undefined;
    }
}
//# sourceMappingURL=context-propagation.js.map