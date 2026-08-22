import type { Context as OpenTelemetryContext } from "@opentelemetry/api";
export declare const STREAM_ID_HEADER = "x-stream-id";
export declare const TRACE_SAMPLING_HEADER = "x-trace";
export declare function newStreamId(): string;
interface ContextState {
    readonly signal: AbortSignal;
    readonly deadline: number | undefined;
    readonly samplingEnabled: boolean;
}
export declare class Context {
    #private;
    constructor(signal?: AbortSignal);
    static background(): Context;
    private static fromState;
    protected state(): ContextState;
    signal(): AbortSignal;
    deadline(): number | undefined;
    remainingMs(): number | undefined;
    cancelled(): boolean;
    samplingEnabled(): boolean;
    withDeadline(deadline: number | undefined): Context;
    bounded(timeoutMs: number): Context;
    withExternalCancellation(signal: AbortSignal): Context;
    withoutCancellation(): Context;
    withSampling(enabled: boolean): Context;
}
export declare class MessageContext extends Context {
    #private;
    constructor(signal?: AbortSignal);
    private static fromMessageState;
    private clone;
    signal(): AbortSignal;
    deadline(): number | undefined;
    remainingMs(): number | undefined;
    cancelled(): boolean;
    samplingEnabled(): boolean;
    withDeadline(deadline: number | undefined): MessageContext;
    bounded(timeoutMs: number): MessageContext;
    withExternalCancellation(signal: AbortSignal): MessageContext;
    withoutCancellation(): MessageContext;
    withSampling(enabled: boolean): MessageContext;
    metadata(): ReadonlyMap<string, string>;
    withMetadata(metadata: ReadonlyMap<string, string>): MessageContext;
    streamId(): string | undefined;
    withStreamId(streamId: string): MessageContext;
    priority(): number | undefined;
    withPriority(priority: number): MessageContext;
    openTelemetryContext(): OpenTelemetryContext | undefined;
    withOpenTelemetryContext(context: OpenTelemetryContext): MessageContext;
    transportMetadata(): ReadonlyMap<string, string>;
}
export {};
//# sourceMappingURL=context.d.ts.map