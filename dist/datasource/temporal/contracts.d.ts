export declare const ENDPOINT_WORKFLOW_TYPE = "servicelib.temporal-endpoint.v1";
export declare function temporalIdentityName(value: string): string;
export declare function temporalEndpointActivityType(connectorName: string, endpointName: string): string;
export declare function temporalDirectWorkflowType(connectorName: string, endpointName: string): string;
export declare function temporalEndpointWorkflowId(connectorName: string, endpointName: string, messageId: string): string;
export interface EndpointEnvelope {
    readonly version: number;
    readonly endpointId: number;
    readonly messageId: string;
    readonly streamId: string;
    readonly priority: number;
    readonly deadlineUnixMillis: number;
    readonly scheduled: boolean;
    readonly scheduleId: string;
    readonly scheduledAtUnixMillis: number;
    readonly firedAtUnixMillis: number;
    readonly payload: Uint8Array;
}
export interface EndpointResult {
    readonly payload: Uint8Array;
}
export interface EndpointWireEnvelope extends Omit<EndpointEnvelope, "payload"> {
    readonly payload: readonly number[];
}
export interface EndpointWireResult {
    readonly payload: readonly number[];
}
export interface EndpointWorkflowRequest {
    readonly executionType: TemporalExecutionType;
    readonly runtimeConfig: CanonicalConfig;
    readonly activityType: string;
    readonly activityStartToCloseTimeout: number;
    readonly activityHeartbeatTimeout: number;
    readonly maximumAttempts: number;
    readonly priority: number;
    readonly envelope: EndpointWireEnvelope;
}
import type { CanonicalConfig, TemporalExecutionType } from "../../runtime/config/index.js";
//# sourceMappingURL=contracts.d.ts.map