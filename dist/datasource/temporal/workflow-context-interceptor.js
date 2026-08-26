import { CancellationScope } from "@temporalio/workflow";
import { defaultPayloadConverter } from "@temporalio/common";
import { MessageContext } from "../../runtime/context.js";
import { TEMPORAL_HEADER_DEADLINE_UNIX_NANO, TEMPORAL_HEADER_PRIORITY } from "./headers.js";
const CARRIER_NAMES = ["traceparent", "tracestate", "baggage", "x-trace", "x-stream-id"];
const TEMPORAL_TRACE_HEADER = "_tracer-data";
let workflowMessageContext;
export function currentTemporalWorkflowMessageContext() {
    if (workflowMessageContext === undefined) {
        throw new Error("Temporal Workflow MessageContext is not initialized");
    }
    return workflowMessageContext;
}
export function interceptors() {
    let carrier = {};
    const inbound = {
        execute(input, next) {
            const headers = withTemporalTraceHeader(input.headers);
            carrier = headers;
            const cancellation = new AbortController();
            try {
                void CancellationScope.current().cancelRequested.catch((reason) => {
                    cancellation.abort(reason instanceof Error ? reason : new Error("Workflow cancelled"));
                });
            }
            catch {
                // Direct interceptor unit tests run outside a Workflow isolate.
            }
            workflowMessageContext = decodeContext(headers).withExternalCancellation(cancellation.signal);
            return next({ ...input, headers });
        }
    };
    const outbound = {
        scheduleActivity(input, next) {
            return next({ ...input, headers: { ...carrier, ...input.headers } });
        }
    };
    return { inbound: [inbound], outbound: [outbound] };
}
function withTemporalTraceHeader(headers) {
    if (headers[TEMPORAL_TRACE_HEADER] !== undefined)
        return headers;
    const traceparent = decodeString(headers["traceparent"]);
    if (traceparent === undefined || traceparent === "")
        return headers;
    const carrier = { traceparent };
    for (const name of ["tracestate", "baggage"]) {
        const value = decodeString(headers[name]);
        if (value !== undefined && value !== "")
            carrier[name] = value;
    }
    return {
        ...headers,
        [TEMPORAL_TRACE_HEADER]: defaultPayloadConverter.toPayload(carrier)
    };
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
            // Invalid external carrier fields are ignored consistently with Activity extraction.
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
//# sourceMappingURL=workflow-context-interceptor.js.map