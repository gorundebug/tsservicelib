import { continueAsNew, scheduleActivity, sleep, workflowInfo } from "@temporalio/workflow";
import { DurableCallContext, TemporalContinueAsNewRequest, runDurableCallWorkflow } from "../../runtime/durable-call-context.js";
import { str } from "../../runtime/environment/log.js";
import { isTemporalEndpointConfig } from "../../runtime/config/schedule.js";
import { scheduledTimeFromWorkflowId } from "./scheduled-time.js";
import { currentTemporalWorkflowMessageContext } from "./workflow-context-interceptor.js";
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
/** Execute one generated service-owned Workflow endpoint in the Workflow isolate. */
export async function executeTemporalWorkflowEndpoint(endpoint) {
    if (endpoint.request.executionType !== "Workflow") {
        throw new Error("direct Temporal Workflow received an Activity endpoint request");
    }
    const envelope = scheduledEnvelope(endpoint.request.envelope);
    const info = workflowInfo();
    const messageId = envelope.messageId || info.workflowId;
    let context = currentTemporalWorkflowMessageContext()
        .withStreamId(envelope.streamId || messageId)
        .withPriority(envelope.priority);
    const endpointConfig = endpoint.environment
        .runtimeConfig()
        .endpointById(endpoint.stream.endpointId());
    if (isTemporalEndpointConfig(endpointConfig) && endpointConfig.tracingEnabled === true) {
        context = context.withSampling(true);
    }
    if (envelope.deadlineUnixMillis > 0) {
        context = context.bounded(Math.max(0, envelope.deadlineUnixMillis - Date.now()));
    }
    const durable = new DurableCallContext(messageId, "Workflow", {
        timer: async (delayMs) => {
            await sleep(delayMs);
        }
    });
    context = context.withDurableCallContext(durable);
    endpoint.environment.log().info(context, "temporal workflow graph started", str("workflow_id", info.workflowId), str("workflow_type", info.workflowType));
    let result;
    if (endpoint.stream.resultStream() !== undefined) {
        result = Promise.withResolvers();
        const consumer = {
            consume: (_context, value) => {
                result?.resolve(value);
            }
        };
        endpoint.stream.setResultConsumer(consumer);
    }
    await endpoint.environment.start();
    try {
        return await runDurableCallWorkflow(durable, async () => {
            await endpoint.activate(context, envelope);
            if (result === undefined) {
                await endpoint.environment.finish();
                return { payload: [] };
            }
            const value = await result.promise;
            const resultStream = endpoint.stream.resultStream();
            if (resultStream === undefined)
                throw new Error("Temporal Workflow result stream disappeared");
            await endpoint.environment.finish();
            return { payload: Array.from(resultStream.serde().serialize(value)) };
        });
    }
    catch (error) {
        try {
            await endpoint.environment.finish();
        }
        catch (cleanupError) {
            throw new AggregateError([error, cleanupError], "Temporal Workflow execution and graph cleanup both failed", { cause: cleanupError });
        }
        if (error instanceof TemporalContinueAsNewRequest) {
            const nextEnvelope = {
                ...envelope,
                scheduled: false,
                scheduleId: "",
                scheduledAtUnixMillis: 0,
                firedAtUnixMillis: 0,
                payload: Array.from(endpoint.stream.serde().serialize(error.nextInput))
            };
            return continueAsNew({ ...endpoint.request, envelope: nextEnvelope });
        }
        throw error;
    }
}
export function scheduledEnvelope(envelope) {
    if (!envelope.scheduled)
        return envelope;
    const info = workflowInfo();
    return {
        ...envelope,
        messageId: info.workflowId,
        streamId: info.workflowId,
        scheduledAtUnixMillis: scheduledTimeFromWorkflowId(info.workflowId, info.startTime),
        firedAtUnixMillis: Date.now()
    };
}
function activityOptions(request) {
    if (request.executionType !== "Activity" || request.activityStartToCloseTimeout < 1) {
        throw new Error("Temporal Activity adapter requires a positive start-to-close timeout");
    }
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