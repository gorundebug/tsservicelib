// The graph API is also bundled into the Temporal isolate, where process and
// Node context propagation do not exist. Never statically import Node here.
const asyncHooks = typeof process === "undefined" ? undefined : process.getBuiltinModule("async_hooks");
const os = typeof process === "undefined" ? undefined : process.getBuiltinModule("os");
export function bindPoolTask(execute) {
    return asyncHooks?.AsyncResource.bind(execute) ?? execute;
}
export function normalizeExecutorsCount(count) {
    if (!Number.isSafeInteger(count) || count < 0)
        throw new RangeError("executorsCount must be a non-negative integer");
    return count === 0 ? (os?.availableParallelism() ?? 1) : count;
}
// One listener per signal, even when a request owns many queued tasks.
const subscriptions = new WeakMap();
export function subscribeAbort(signal, callback) {
    if (signal.aborted) {
        callback();
        return () => undefined;
    }
    let subscription = subscriptions.get(signal);
    if (subscription === undefined) {
        const callbacks = new Set();
        const listener = () => {
            subscriptions.delete(signal);
            for (const notify of callbacks)
                notify();
            callbacks.clear();
        };
        subscription = { callbacks, listener };
        subscriptions.set(signal, subscription);
        signal.addEventListener("abort", listener, { once: true });
    }
    const current = subscription;
    current.callbacks.add(callback);
    return () => {
        current.callbacks.delete(callback);
        if (current.callbacks.size === 0) {
            signal.removeEventListener("abort", current.listener);
            if (subscriptions.get(signal) === current)
                subscriptions.delete(signal);
        }
    };
}
export function reportPoolError(handler, error) {
    try {
        handler(error);
    }
    catch {
        /* An error reporter must not strand accepted work. */
    }
}
//# sourceMappingURL=pool-support.js.map