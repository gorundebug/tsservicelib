import { scheduleActivity, workflowInfo } from "@temporalio/workflow";
import { scheduledTimeFromWorkflowId } from "./scheduled-time.js";
export async function servicelibTemporalEndpointV1(request) {
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
    return scheduleActivity(request.activityType, [envelope], activityOptions(request));
}
export { servicelibTemporalEndpointV1 as "servicelib.temporal-endpoint.v1" };
function activityOptions(request) {
    return {
        startToCloseTimeout: request.activityStartToCloseTimeout,
        ...(request.activityHeartbeatTimeout > 0
            ? { heartbeatTimeout: request.activityHeartbeatTimeout }
            : {}),
        retry: { maximumAttempts: request.maximumAttempts },
        priority: { priorityKey: request.priority }
    };
}
//# sourceMappingURL=workflows.js.map