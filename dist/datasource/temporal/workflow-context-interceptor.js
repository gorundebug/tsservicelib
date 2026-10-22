import { CancellationScope } from "@temporalio/workflow";
import { defaultPayloadConverter } from "@temporalio/common";
import { MessageContext } from "../../runtime/context.js";
import { TEMPORAL_HEADER_DEADLINE_UNIX_NANO, TEMPORAL_HEADER_PRIORITY } from "./headers.js";
const CARRIER_NAMES = ["traceparent", "tracestate", "baggage", "x-trace", "x-stream-id"];
const STREAM_ID_ONLY_NAMES = ["x-stream-id"];
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
    let tracingEnabled = true;
    const inbound = {
        execute(input, next) {
            tracingEnabled = workflowTracingEnabled(input);
            const headers = tracingEnabled
                ? withTemporalTraceHeader(input.headers)
                : withoutTracingHeaders(input.headers);
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
            workflowMessageContext = decodeContext(headers, tracingEnabled).withExternalCancellation(cancellation.signal);
            return next({ ...input, headers });
        }
    };
    const outbound = {
        scheduleActivity(input, next) {
            const headers = { ...carrier, ...input.headers };
            return next({
                ...input,
                headers: tracingEnabled ? headers : withoutTracingHeaders(headers)
            });
        }
    };
    return { inbound: [inbound], outbound: [outbound] };
}
function workflowTracingEnabled(input) {
    const request = input.args?.[0];
    if (request === null || typeof request !== "object")
        return true;
    const telemetry = request.telemetry;
    return telemetry?.noopTracing !== true;
}
function withoutTracingHeaders(headers) {
    const filtered = { ...headers };
    for (const name of ["traceparent", "tracestate", "baggage", "x-trace", TEMPORAL_TRACE_HEADER]) {
        Reflect.deleteProperty(filtered, name);
    }
    return filtered;
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
function decodeContext(headers, tracingEnabled) {
    const metadata = new Map();
    for (const name of tracingEnabled ? CARRIER_NAMES : STREAM_ID_ONLY_NAMES) {
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