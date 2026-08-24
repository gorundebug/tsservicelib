import { scheduleActivity, workflowInfo } from "@temporalio/workflow";
export async function servicegenDurableLinkV1(request) {
    await scheduleActivity(request.activityType, [request.envelope], activityOptions(request));
}
export async function servicegenTemporalEndpointV1(request) {
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
    return scheduleActivity(request.activityType, [envelope], activityOptions(request));
}
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