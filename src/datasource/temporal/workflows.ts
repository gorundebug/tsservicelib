import { scheduleActivity, workflowInfo } from "@temporalio/workflow";

import type {
  DurableWorkflowRequest,
  EndpointWireResult,
  EndpointWorkflowRequest
} from "./contracts.js";
import { scheduledTimeFromWorkflowId } from "./scheduled-time.js";

export async function servicelibDurableLinkV1(request: DurableWorkflowRequest): Promise<void> {
  await scheduleActivity(request.activityType, [request.envelope], activityOptions(request));
}

export async function servicelibTemporalEndpointV1(
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
  servicelibDurableLinkV1 as "servicelib.durable-link.v1",
  servicelibTemporalEndpointV1 as "servicelib.temporal-endpoint.v1"
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
