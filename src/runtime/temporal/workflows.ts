import { scheduleActivity, workflowInfo } from "@temporalio/workflow";

import type {
  DurableWorkflowRequest,
  EndpointResult,
  EndpointWorkflowRequest
} from "./contracts.js";

export async function servicegenDurableLinkV1(request: DurableWorkflowRequest): Promise<void> {
  await scheduleActivity(request.activityType, [request.envelope], activityOptions(request));
}

export async function servicegenTemporalEndpointV1(
  request: EndpointWorkflowRequest
): Promise<EndpointResult> {
  let envelope = request.envelope;
  if (envelope.scheduled) {
    const info = workflowInfo();
    envelope = {
      ...envelope,
      executionId: info.workflowId,
      streamId: info.workflowId,
      scheduledAtUnixMillis: info.startTime.getTime(),
      firedAtUnixMillis: Date.now()
    };
  }
  return scheduleActivity<EndpointResult>(
    request.activityType,
    [envelope],
    activityOptions(request)
  );
}

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
