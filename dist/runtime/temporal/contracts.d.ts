import type { DurableEnvelope } from "../durable.js";
export declare const DURABLE_WORKFLOW_TYPE = "servicegen.durable-link.v1";
export declare const ENDPOINT_WORKFLOW_TYPE = "servicegen.temporal-endpoint.v1";
export interface EndpointEnvelope {
    readonly version: number;
    readonly endpointId: number;
    readonly executionId: string;
    readonly streamId: string;
    readonly priority: number;
    readonly deadlineUnixMillis: number;
    readonly samplingEnabled: boolean;
    readonly scheduled: boolean;
    readonly scheduleId: string;
    readonly scheduledAtUnixMillis: number;
    readonly firedAtUnixMillis: number;
    readonly payload: Uint8Array;
}
export interface EndpointResult {
    readonly payload: Uint8Array;
}
export interface TemporalActivityPolicy {
    readonly activityType: string;
    readonly activityStartToCloseTimeout: number;
    readonly activityHeartbeatTimeout: number;
    readonly maximumAttempts: number;
    readonly priority: number;
}
export interface DurableWorkflowRequest extends TemporalActivityPolicy {
    readonly envelope: DurableEnvelope;
}
export interface EndpointWorkflowRequest extends TemporalActivityPolicy {
    readonly envelope: EndpointEnvelope;
}
//# sourceMappingURL=contracts.d.ts.map