import { DataConnectorType } from "./types.js";
function isNonNegativeInteger(value) {
    return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function isPositiveInteger(value) {
    return isNonNegativeInteger(value) && value > 0;
}
export function isCronDataConnectorConfig(value) {
    return value?.type === DataConnectorType.Cron;
}
export function requireCronDataConnectorConfig(value) {
    if (!isCronDataConnectorConfig(value)) {
        throw new Error("invalid Cron data connector config");
    }
    return value;
}
export function isTemporalDataConnectorConfig(value) {
    return (value?.type === DataConnectorType.Temporal &&
        "address" in value &&
        typeof value.address === "string" &&
        value.address.length > 0 &&
        "namespace" in value &&
        typeof value.namespace === "string" &&
        value.namespace.length > 0 &&
        "identity" in value &&
        typeof value.identity === "string" &&
        "apiKey" in value &&
        typeof value.apiKey === "string" &&
        "tlsEnabled" in value &&
        typeof value.tlsEnabled === "boolean" &&
        "tlsServerName" in value &&
        typeof value.tlsServerName === "string" &&
        "tlsCaFile" in value &&
        typeof value.tlsCaFile === "string" &&
        "tlsCertFile" in value &&
        typeof value.tlsCertFile === "string" &&
        "tlsKeyFile" in value &&
        typeof value.tlsKeyFile === "string" &&
        "workerStopTimeout" in value &&
        isNonNegativeInteger(value.workerStopTimeout));
}
export function requireTemporalDataConnectorConfig(value) {
    if (!isTemporalDataConnectorConfig(value)) {
        throw new Error("invalid Temporal data connector config");
    }
    return value;
}
export function isCronEndpointConfig(value) {
    return (value !== undefined &&
        "enabled" in value &&
        typeof value.enabled === "boolean" &&
        "schedule" in value &&
        typeof value.schedule === "string" &&
        "timezone" in value &&
        value.timezone === "UTC" &&
        "overlapPolicy" in value &&
        (value.overlapPolicy === "Allow" || value.overlapPolicy === "Skip") &&
        "missedRunPolicy" in value &&
        (value.missedRunPolicy === "FireOnce" || value.missedRunPolicy === "Skip"));
}
export function requireCronEndpointConfig(value) {
    if (!isCronEndpointConfig(value)) {
        throw new Error("invalid Cron endpoint config");
    }
    return value;
}
export function isTemporalEndpointConfig(value) {
    return (isCronEndpointConfig(value) &&
        "taskQueue" in value &&
        typeof value.taskQueue === "string" &&
        value.taskQueue.length > 0 &&
        "temporalExecutionType" in value &&
        (value.temporalExecutionType === "Activity" || value.temporalExecutionType === "Workflow") &&
        "maxConcurrentActivities" in value &&
        isNonNegativeInteger(value.maxConcurrentActivities) &&
        (value.temporalExecutionType !== "Activity" || value.maxConcurrentActivities > 0) &&
        "maxConcurrentWorkflowTasks" in value &&
        isNonNegativeInteger(value.maxConcurrentWorkflowTasks) &&
        (value.temporalExecutionType !== "Workflow" || value.maxConcurrentWorkflowTasks > 0) &&
        "scheduleId" in value &&
        typeof value.scheduleId === "string" &&
        "workflowExecutionTimeout" in value &&
        isNonNegativeInteger(value.workflowExecutionTimeout) &&
        "activityStartToCloseTimeout" in value &&
        isNonNegativeInteger(value.activityStartToCloseTimeout) &&
        (value.temporalExecutionType !== "Activity" || value.activityStartToCloseTimeout > 0) &&
        "activityHeartbeatTimeout" in value &&
        isNonNegativeInteger(value.activityHeartbeatTimeout) &&
        "maximumAttempts" in value &&
        isPositiveInteger(value.maximumAttempts));
}
export function requireTemporalEndpointConfig(value) {
    if (!isTemporalEndpointConfig(value)) {
        throw new Error("invalid Temporal endpoint config");
    }
    return value;
}
//# sourceMappingURL=schedule.js.map