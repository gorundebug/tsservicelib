/** Observe every operation, but stop waiting when the shared shutdown context expires.
 * An undefined result means the operation still owns its resources and may finish later.
 */
export async function settleWithinDeadline(context, operations) {
    if (operations.length === 0)
        return [];
    const results = new Array(operations.length);
    const tracked = operations.map(async (operation, index) => {
        try {
            results[index] = { status: "fulfilled", value: await operation };
        }
        catch (reason) {
            results[index] = { status: "rejected", reason };
        }
    });
    if (context.cancelled())
        return [...results];
    const signal = context.signal();
    const remainingMs = context.remainingMs();
    let timer;
    let onAbort = () => undefined;
    const cancelled = new Promise((resolve) => {
        onAbort = resolve;
        signal.addEventListener("abort", onAbort, { once: true });
        if (remainingMs !== undefined) {
            const checkDeadline = () => {
                const remaining = context.remainingMs();
                if (remaining !== undefined && remaining > 0) {
                    // Node truncates fractional delays. Never publish a timeout before
                    // the caller's actual deadline, even if the timer wakes up early.
                    timer = setTimeout(checkDeadline, Math.max(1, Math.ceil(remaining)));
                }
                else {
                    resolve();
                }
            };
            timer = setTimeout(checkDeadline, Math.max(0, Math.ceil(remainingMs)));
        }
        if (signal.aborted)
            resolve();
    });
    try {
        await Promise.race([Promise.all(tracked), cancelled]);
        return [...results];
    }
    finally {
        if (timer !== undefined)
            clearTimeout(timer);
        signal.removeEventListener("abort", onAbort);
    }
}
//# sourceMappingURL=lifecycle.js.map