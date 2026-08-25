import { executeChild, scheduleActivity, workflowInfo } from "@temporalio/workflow";
import { isTemporalEndpointConfig } from "../../runtime/config/index.js";
import { normalizeTemporalPriority } from "../../runtime/schedule.js";
import { temporalDirectWorkflowType, temporalEndpointActivityType, temporalEndpointWorkflowId, temporalIdentityName } from "./contracts.js";
/** Attach a plain Temporal sink to a graph executing inside a Workflow isolate. */
export function makeTemporalWorkflowSinkEndpointConsumer(stream) {
    return makeWorkflowSinkConsumer(stream, false);
}
/** Attach a result-producing Temporal sink to a Workflow graph. */
export function makeTemporalWorkflowSinkEndpointConsumerWithResult(stream) {
    return makeWorkflowSinkConsumer(stream, true);
}
function makeWorkflowSinkConsumer(stream, withResult) {
    const environment = stream.runtimeEnvironment();
    const endpoint = environment.runtimeConfig().endpointById(stream.endpointId());
    if (!isTemporalEndpointConfig(endpoint)) {
        throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
    }
    const connector = environment.runtimeConfig().dataConnectorById(endpoint.idDataConnector);
    if (connector === undefined) {
        throw new Error(`Temporal connector ${String(endpoint.idDataConnector)} not found`);
    }
    const consumer = {
        consume: async (context, value) => {
            const parentId = context.streamId() ?? workflowInfo().workflowId;
            const messageId = `${parentId}/${temporalIdentityName(stream.name)}`;
            const remainingMs = context.remainingMs();
            const envelope = {
                version: 1,
                endpointId: endpoint.id,
                messageId,
                streamId: parentId,
                priority: context.priority() ?? 0,
                deadlineUnixMillis: remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
                scheduled: false,
                scheduleId: "",
                scheduledAtUnixMillis: 0,
                firedAtUnixMillis: 0,
                payload: Array.from(stream.inputSerde().serialize(value))
            };
            const result = await executeEndpoint(endpoint, connector.name, environment.runtimeConfig().config(), envelope);
            if (withResult) {
                const resultStream = stream;
                await resultStream.consumeResult(context, resultStream.serde().deserialize(Uint8Array.from(result.payload)));
            }
        }
    };
    stream.setSinkConsumer(consumer);
    return consumer;
}
async function executeEndpoint(endpoint, connectorName, runtimeConfig, envelope) {
    const request = {
        executionType: endpoint.temporalExecutionType,
        runtimeConfig,
        activityType: temporalEndpointActivityType(connectorName, endpoint.name),
        activityStartToCloseTimeout: endpoint.activityStartToCloseTimeout,
        activityHeartbeatTimeout: endpoint.activityHeartbeatTimeout,
        maximumAttempts: endpoint.maximumAttempts,
        priority: normalizeTemporalPriority(envelope.priority),
        envelope
    };
    if (endpoint.temporalExecutionType === "Activity") {
        return scheduleActivity(request.activityType, [envelope], {
            taskQueue: endpoint.taskQueue,
            startToCloseTimeout: endpoint.activityStartToCloseTimeout,
            ...(endpoint.activityHeartbeatTimeout > 0
                ? { heartbeatTimeout: endpoint.activityHeartbeatTimeout }
                : {}),
            retry: { maximumAttempts: endpoint.maximumAttempts },
            priority: { priorityKey: request.priority }
        });
    }
    return executeChild(temporalDirectWorkflowType(connectorName, endpoint.name), {
        args: [request],
        workflowId: temporalEndpointWorkflowId(connectorName, endpoint.name, envelope.messageId),
        taskQueue: endpoint.taskQueue,
        ...(endpoint.workflowExecutionTimeout > 0
            ? { workflowExecutionTimeout: endpoint.workflowExecutionTimeout }
            : {})
    });
}
//# sourceMappingURL=workflow-sink.js.map