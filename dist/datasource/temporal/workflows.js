import { scheduleActivity, sleep, workflowInfo } from "@temporalio/workflow";
import { scheduledTimeFromWorkflowId } from "./scheduled-time.js";
export async function servicelibDurableLinkV1(request) {
    const result = await scheduleActivity(request.activityType, [request.envelope], activityOptions(request));
    await runDurableContinuations(request, result);
}
export async function servicelibTemporalEndpointV1(request) {
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
    const result = await scheduleActivity(request.activityType, [envelope], activityOptions(request));
    await runDurableContinuations(request, result.durable);
    return result.result;
}
async function runDurableContinuations(request, initial) {
    let result = initial;
    while (result.continuation !== undefined) {
        const continuation = result.continuation;
        const delayMs = continuation.wakeAtUnixMillis - Date.now();
        if (delayMs > 0)
            await sleep(delayMs);
        result = await scheduleActivity(request.continuationActivityType, [continuation], activityOptions(request));
    }
}
// Temporal dispatches a Workflow by the exported bundle key. Keep the
// cross-language contract names stable without leaking them into graph nodes.
export { servicelibDurableLinkV1 as "servicelib.durable-link.v1", servicelibTemporalEndpointV1 as "servicelib.temporal-endpoint.v1" };
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