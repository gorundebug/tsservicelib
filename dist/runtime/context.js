import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
export const STREAM_ID_HEADER = "x-stream-id";
export const TRACE_SAMPLING_HEADER = "x-trace";
export function newStreamId() {
    return randomUUID();
}
const EMPTY_METADATA = new Map();
function deadlineSignal(deadline) {
    if (deadline === undefined) {
        return undefined;
    }
    return AbortSignal.timeout(Math.max(0, Math.ceil(deadline - performance.now())));
}
function composeSignal(signal, deadline) {
    const timeoutSignal = deadlineSignal(deadline);
    return timeoutSignal === undefined ? signal : AbortSignal.any([signal, timeoutSignal]);
}
function boundedDeadline(current, timeoutMs) {
    const candidate = performance.now() + Math.max(0, timeoutMs);
    return current === undefined ? candidate : Math.min(current, candidate);
}
function childDeadline(current, requested) {
    if (requested === undefined)
        return current;
    return current === undefined ? requested : Math.min(current, requested);
}
export class Context {
    #state;
    constructor(signal = new AbortController().signal) {
        this.#state = { signal, deadline: undefined, samplingEnabled: false };
    }
    static background() {
        return new Context();
    }
    static fromState(state) {
        const context = new Context(state.signal);
        context.#state = state;
        return context;
    }
    state() {
        return this.#state;
    }
    signal() {
        return this.#state.signal;
    }
    deadline() {
        return this.#state.deadline;
    }
    remainingMs() {
        return this.#state.deadline === undefined
            ? undefined
            : Math.max(0, this.#state.deadline - performance.now());
    }
    cancelled() {
        return this.#state.signal.aborted || (this.#state.deadline ?? Infinity) <= performance.now();
    }
    samplingEnabled() {
        return this.#state.samplingEnabled;
    }
    withDeadline(deadline) {
        const effective = childDeadline(this.#state.deadline, deadline);
        if (effective === this.#state.deadline)
            return this;
        return Context.fromState({
            ...this.#state,
            deadline: effective,
            signal: composeSignal(this.#state.signal, effective)
        });
    }
    bounded(timeoutMs) {
        return this.withDeadline(boundedDeadline(this.#state.deadline, timeoutMs));
    }
    withExternalCancellation(signal) {
        if (signal === this.#state.signal)
            return this;
        return Context.fromState({
            ...this.#state,
            signal: AbortSignal.any([this.#state.signal, signal])
        });
    }
    withoutCancellation() {
        return Context.fromState({
            ...this.#state,
            signal: new AbortController().signal,
            deadline: undefined
        });
    }
    withSampling(enabled) {
        return Context.fromState({ ...this.#state, samplingEnabled: enabled });
    }
}
export class MessageContext extends Context {
    #messageState;
    constructor(signal = new AbortController().signal) {
        super(signal);
        this.#messageState = {
            ...this.state(),
            durableCallContext: undefined,
            metadata: undefined,
            openTelemetryContext: undefined,
            priority: undefined
        };
    }
    static fromMessageState(state) {
        const context = new MessageContext(state.signal);
        context.#messageState = state;
        return context;
    }
    clone(changes) {
        return MessageContext.fromMessageState({ ...this.#messageState, ...changes });
    }
    signal() {
        return this.#messageState.signal;
    }
    deadline() {
        return this.#messageState.deadline;
    }
    remainingMs() {
        return this.#messageState.deadline === undefined
            ? undefined
            : Math.max(0, this.#messageState.deadline - performance.now());
    }
    cancelled() {
        return (this.#messageState.signal.aborted ||
            (this.#messageState.deadline ?? Infinity) <= performance.now());
    }
    samplingEnabled() {
        return this.#messageState.samplingEnabled;
    }
    withDeadline(deadline) {
        const effective = childDeadline(this.#messageState.deadline, deadline);
        if (effective === this.#messageState.deadline)
            return this;
        return this.clone({
            deadline: effective,
            signal: composeSignal(this.#messageState.signal, effective)
        });
    }
    bounded(timeoutMs) {
        return this.withDeadline(boundedDeadline(this.#messageState.deadline, timeoutMs));
    }
    withExternalCancellation(signal) {
        if (signal === this.#messageState.signal)
            return this;
        return this.clone({ signal: AbortSignal.any([this.#messageState.signal, signal]) });
    }
    withoutCancellation() {
        return this.clone({
            signal: new AbortController().signal,
            deadline: undefined
        });
    }
    withSampling(enabled) {
        const metadata = new Map(this.metadata());
        if (enabled) {
            metadata.set(TRACE_SAMPLING_HEADER, "1");
        }
        else {
            metadata.delete(TRACE_SAMPLING_HEADER);
        }
        return this.clone({
            metadata: metadata.size === 0 ? undefined : metadata,
            samplingEnabled: enabled
        });
    }
    metadata() {
        return this.#messageState.metadata ?? EMPTY_METADATA;
    }
    withMetadata(metadata) {
        const copy = new Map(metadata);
        const samplingEnabled = (copy.get(TRACE_SAMPLING_HEADER)?.length ?? 0) > 0 ||
            traceparentIsSampled(copy.get("traceparent"));
        return this.clone({ metadata: copy.size === 0 ? undefined : copy, samplingEnabled });
    }
    streamId() {
        return this.#messageState.metadata?.get(STREAM_ID_HEADER);
    }
    withStreamId(streamId) {
        const metadata = new Map(this.metadata());
        metadata.set(STREAM_ID_HEADER, streamId);
        return this.clone({ metadata });
    }
    priority() {
        return this.#messageState.priority;
    }
    withPriority(priority) {
        return this.clone({ priority });
    }
    openTelemetryContext() {
        return this.#messageState.openTelemetryContext;
    }
    withOpenTelemetryContext(context) {
        return this.clone({ openTelemetryContext: context });
    }
    transportMetadata() {
        const result = new Map();
        for (const name of [
            STREAM_ID_HEADER,
            TRACE_SAMPLING_HEADER,
            "traceparent",
            "tracestate",
            "baggage"
        ]) {
            const value = this.#messageState.metadata?.get(name);
            if (value !== undefined) {
                result.set(name, value);
            }
        }
        return result;
    }
    /** @internal Attaches processing-side Activity state without serializing it. */
    withDurableCallContext(durable) {
        return this.clone({ durableCallContext: durable });
    }
    /** @internal Returns local state owned by the receiving Activity adapter. */
    durableCallContext() {
        return this.#messageState.durableCallContext;
    }
}
function traceparentIsSampled(value) {
    if (value === undefined) {
        return false;
    }
    const fields = value.split("-");
    const flags = fields[3];
    if (fields.length !== 4 || flags?.length !== 2) {
        return false;
    }
    const parsed = Number.parseInt(flags, 16);
    return Number.isFinite(parsed) && (parsed & 1) === 1;
}
//# sourceMappingURL=context.js.map