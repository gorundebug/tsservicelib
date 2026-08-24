const scheduleWorkflowIdSuffix = /-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)$/u;
export function scheduledTimeFromWorkflowId(workflowId, fallback) {
    const match = scheduleWorkflowIdSuffix.exec(workflowId);
    if (match?.[1] !== undefined) {
        const value = Date.parse(match[1]);
        if (Number.isFinite(value))
            return value;
    }
    return fallback.getTime();
}
//# sourceMappingURL=scheduled-time.js.map