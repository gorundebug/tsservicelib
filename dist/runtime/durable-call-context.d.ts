import type { MessageContext } from "./context.js";
import { type Span } from "./environment/tracing/index.js";
export declare const DurableCallEvent: {
    readonly Heartbeat: "heartbeat";
    readonly Success: "success";
    readonly Error: "error";
    readonly MissingOutcome: "missing_outcome";
    readonly DuplicateTerminal: "duplicate_terminal";
    readonly LateHeartbeat: "late_heartbeat";
    readonly Suspended: "suspended";
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
export interface DurableContinuation {
    readonly version: 1;
    readonly fromName: string;
    readonly toName: string;
    readonly callId: string;
    readonly streamId: string;
    readonly priority: number;
    readonly deadlineUnixMillis: number;
    readonly wakeAtUnixMillis: number;
    readonly payload: Uint8Array;
}
export interface DurableActivityResult {
    readonly continuation?: DurableContinuation;
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
    wait(): Promise<DurableActivityResult>;
    beginDelay(delayMs: number): void;
    captureContinuation(context: MessageContext, fromName: string, toName: string, payload: Uint8Array): boolean;
    finishSpan(): void;
    private complete;
    private report;
}
export declare function durableCallHeartbeat(context: MessageContext, message: unknown): void;
export declare function durableCallSuccess(context: MessageContext): void;
export declare function durableCallError(context: MessageContext, error: Error): void;
export declare function beginDurableDelay(context: MessageContext, delayMs: number): boolean;
export declare function captureDurableContinuation(context: MessageContext, fromName: string, toName: string, payload: Uint8Array): boolean;
export declare function bindDurableCallSpan(context: MessageContext, span: Span): boolean;
export declare function runDurableCallActivity(signal: AbortSignal, durable: DurableCallContext, invoke: () => Promise<void>): Promise<DurableActivityResult>;
//# sourceMappingURL=durable-call-context.d.ts.map