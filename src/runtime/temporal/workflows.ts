import { scheduleActivity, workflowInfo } from "@temporalio/workflow";

import type {
  DurableWorkflowRequest,
  EndpointWireResult,
  EndpointWorkflowRequest
} from "./contracts.js";
import { scheduledTimeFromWorkflowId } from "./scheduled-time.js";

export async function servicegenDurableLinkV1(request: DurableWorkflowRequest): Promise<void> {
  await scheduleActivity(request.activityType, [request.envelope], activityOptions(request));
}

export async function servicegenTemporalEndpointV1(
  request: EndpointWorkflowRequest
): Promise<EndpointWireResult> {
  let envelope = request.envelope;
  if (envelope.scheduled) {
    const info = workflowInfo();
    envelope = {
      ...envelope,
      executionId: info.workflowId,
      streamId: info.workflowId,
      scheduledAtUnixMillis: scheduledTimeFromWorkflowId(info.workflowId, info.startTime),
      firedAtUnixMillis: Date.now()
    };
  }
  return scheduleActivity<EndpointWireResult>(
    request.activityType,
    [envelope],
    activityOptions(request)
  );
}

// Temporal dispatches a Workflow by the exported bundle key. Keep the
// cross-language contract names stable without leaking them into graph nodes.
export {
  servicegenDurableLinkV1 as "servicegen.durable-link.v1",
  servicegenTemporalEndpointV1 as "servicegen.temporal-endpoint.v1"
};

function activityOptions(request: {
  readonly activityStartToCloseTimeout: number;
  readonly activityHeartbeatTimeout: number;
  readonly maximumAttempts: number;
  readonly priority: number;
}): {
  readonly startToCloseTimeout: number;
  readonly heartbeatTimeout?: number;
  readonly retry: { readonly maximumAttempts: number };
  readonly priority: { readonly priorityKey: number };
} {
  return {
    startToCloseTimeout: request.activityStartToCloseTimeout,
    ...(request.activityHeartbeatTimeout > 0
      ? { heartbeatTimeout: request.activityHeartbeatTimeout }
      : {}),
    retry: { maximumAttempts: request.maximumAttempts },
    priority: { priorityKey: request.priority }
  };
}
