import type { DurableEnvelope } from "../durable.js";

export const DURABLE_WORKFLOW_TYPE = "servicegen.durable-link.v1";
export const ENDPOINT_WORKFLOW_TYPE = "servicegen.temporal-endpoint.v1";

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

// Temporal's default JSON payload converter does not preserve nested
// Uint8Array values. Wire envelopes therefore use plain byte arrays while the
// graph-facing runtime keeps the idiomatic Uint8Array representation.
export interface DurableWireEnvelope extends Omit<DurableEnvelope, "payload"> {
  readonly payload: readonly number[];
}

export interface EndpointWireEnvelope extends Omit<EndpointEnvelope, "payload"> {
  readonly payload: readonly number[];
}

export interface EndpointWireResult {
  readonly payload: readonly number[];
}

export interface TemporalActivityPolicy {
  readonly activityType: string;
  readonly activityStartToCloseTimeout: number;
  readonly activityHeartbeatTimeout: number;
  readonly maximumAttempts: number;
  readonly priority: number;
}

export interface DurableWorkflowRequest extends TemporalActivityPolicy {
  readonly envelope: DurableWireEnvelope;
}

export interface EndpointWorkflowRequest extends TemporalActivityPolicy {
  readonly envelope: EndpointWireEnvelope;
}
