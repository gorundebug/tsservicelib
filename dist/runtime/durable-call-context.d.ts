import type { MessageContext } from "./context.js";
import { type Span } from "./environment/tracing/index.js";
export declare const DurableCallEvent: {
    readonly Heartbeat: "heartbeat";
    readonly Success: "success";
    readonly Error: "error";
    readonly LateHeartbeat: "late_heartbeat";
};
export type DurableCallEvent = (typeof DurableCallEvent)[keyof typeof DurableCallEvent];
export type DurableCallDiagnostics = (event: DurableCallEvent, error?: Error) => void;
export type DurableCallHeartbeatRecorder = (message: unknown) => void;
export declare class DurableCallContextError extends Error {
}
export declare class DurableCallHeartbeatAfterCompletionError extends DurableCallContextError {
}
/** Processing-side state for one Temporal endpoint Activity. */
export declare class DurableCallContext {
    #private;
    readonly messageId: string;
    constructor(messageId: string, heartbeat?: DurableCallHeartbeatRecorder, diagnostics?: DurableCallDiagnostics);
    bindSpan(span: Span): void;
    heartbeat(message: unknown): void;
    close(error?: Error): void;
    private report;
}
export declare function durableCallHeartbeat(context: MessageContext, message: unknown): void;
export declare function bindDurableCallSpan(context: MessageContext, span: Span): boolean;
export declare function runDurableCallActivity<T>(durable: DurableCallContext, invoke: () => Promise<T>): Promise<T>;
//# sourceMappingURL=durable-call-context.d.ts.map