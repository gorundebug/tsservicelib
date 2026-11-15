export function resolveSubmissionQueue(configured, execution, options) {
    const queue = options.taskQueue ?? "";
    if (queue !== "" && (queue.trim() === "" || queue.includes("\0"))) {
        throw new Error("Temporal submission task queue is invalid");
    }
    const timeout = options.scheduleToStartTimeout ?? 0;
    if (!Number.isFinite(timeout) || timeout < 0) {
        throw new Error("Temporal submission schedule-to-start timeout must not be negative or non-finite");
    }
    if (timeout !== 0 && execution !== "Activity") {
        throw new Error("Temporal submission schedule-to-start timeout requires an Activity endpoint");
    }
    return queue || configured;
}
//# sourceMappingURL=submission-options.js.map