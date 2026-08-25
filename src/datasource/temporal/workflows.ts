import { scheduleActivity, workflowInfo } from "@temporalio/workflow";

import type { EndpointWireResult, EndpointWorkflowRequest } from "./contracts.js";
import { scheduledTimeFromWorkflowId } from "./scheduled-time.js";

export async function servicelibTemporalEndpointV1(
  request: EndpointWorkflowRequest
): Promise<EndpointWireResult> {
  let envelope = request.envelope;
  if (envelope.scheduled) {
    const info = workflowInfo();
    envelope = {
      ...envelope,
      messageId: info.workflowId,
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

export { servicelibTemporalEndpointV1 as "servicelib.temporal-endpoint.v1" };

function activityOptions(request: EndpointWorkflowRequest): {
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
