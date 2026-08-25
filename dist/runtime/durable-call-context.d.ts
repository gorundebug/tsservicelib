import type { MessageContext } from "./context.js";
import { type Span } from "./environment/tracing/index.js";
export declare const DurableCallEvent: {
    readonly Heartbeat: "heartbeat";
    readonly Success: "success";
    readonly Error: "error";
    readonly MissingOutcome: "missing_outcome";
    readonly DuplicateTerminal: "duplicate_terminal";
    readonly LateHeartbeat: "late_heartbeat";
};
export type DurableCallEvent = (typeof DurableCallEvent)[keyof typeof DurableCallEvent];
export type DurableCallDiagnostics = (event: DurableCallEvent, error?: Error) => void;
export type DurableCallHeartbeatRecorder = (message: unknown) => void;
export declare class DurableCallContextError extends Error {
}
export declare class NoDurableCallContextError extends DurableCallContextError {
}
export declare class DurableCallAlreadyCompletedError extends DurableCallContextError {
}
export declare class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {
}
export declare class DurableCallOutcomeMissingError extends DurableCallContextError {
}
export declare class DurableCallContext {
    #private;
    readonly parentCallId: string;
    constructor(parentCallId: string, heartbeat?: DurableCallHeartbeatRecorder, diagnostics?: DurableCallDiagnostics);
    occurrence(key: string): number;
    bindSpan(span: Span): void;
    heartbeat(message: unknown): void;
    success(): void;
    fail(error: Error): void;
    cancelWithoutOutcome(cause: unknown): void;
    wait(): Promise<void>;
    finishSpan(): void;
    private complete;
    private report;
}
export declare function durableCallHeartbeat(context: MessageContext, message: unknown): void;
export declare function durableCallSuccess(context: MessageContext): void;
export declare function durableCallError(context: MessageContext, error: Error): void;
export declare function bindDurableCallSpan(context: MessageContext, span: Span): boolean;
export declare function runDurableCallActivity(signal: AbortSignal, durable: DurableCallContext, invoke: () => Promise<void>): Promise<void>;
//# sourceMappingURL=durable-call-context.d.ts.map