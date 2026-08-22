import { RuntimeDrainTimeoutError, RuntimeStoppedError, errorFromUnknown } from "./errors.js";
export class RuntimeTaskRegistry {
    #controller = new AbortController();
    #tasks = new Map();
    #onError;
    #accepting = true;
    #nextId = 1;
    constructor(onError = () => undefined) {
        this.#onError = onError;
    }
    accepting() {
        return this.#accepting;
    }
    activeCount() {
        return this.#tasks.size;
    }
    admit(task, externalSignal) {
        if (!this.#accepting) {
            return Promise.reject(new RuntimeStoppedError());
        }
        let admitted;
        this.startTask(task, externalSignal, (promise) => {
            admitted = promise;
        });
        if (admitted === undefined) {
            throw new Error("runtime task admission did not produce a promise");
        }
        return admitted;
    }
    admitDetached(task, externalSignal) {
        if (!this.#accepting) {
            this.#onError(new RuntimeStoppedError());
            return;
        }
        this.startTask(task, externalSignal, () => undefined);
    }
    startTask(task, externalSignal, accepted) {
        const id = this.#nextId++;
        const signal = externalSignal === undefined
            ? this.#controller.signal
            : AbortSignal.any([this.#controller.signal, externalSignal]);
        let promise;
        try {
            promise = task(signal);
        }
        catch (error) {
            promise = Promise.reject(errorFromUnknown(error));
        }
        accepted(promise);
        const observed = promise.then(() => {
            this.#tasks.delete(id);
        }, (value) => {
            // The task promise is already observed at this boundary. A faulty
            // reporting callback must not manufacture a second unhandled
            // rejection while the original promise remains visible to admit().
            try {
                this.#onError(errorFromUnknown(value));
            }
            catch {
                // Error reporters are terminal sinks. There is no second canonical
                // destination to which their own failure could be routed safely.
            }
            this.#tasks.delete(id);
        });
        this.#tasks.set(id, observed);
    }
    stopAdmission() {
        this.#accepting = false;
    }
    cancel(reason = new RuntimeStoppedError("runtime work was cancelled")) {
        this.#accepting = false;
        this.#controller.abort(reason);
    }
    async drain(timeoutMs) {
        let timer;
        const timeout = timeoutMs === undefined
            ? undefined
            : new Promise((_resolve, reject) => {
                timer = setTimeout(() => {
                    reject(new RuntimeDrainTimeoutError(timeoutMs));
                }, timeoutMs);
            });
        try {
            while (this.#tasks.size > 0) {
                const snapshot = Promise.allSettled([...this.#tasks.values()]).then(() => undefined);
                if (timeout === undefined) {
                    await snapshot;
                    continue;
                }
                await Promise.race([snapshot, timeout]);
            }
        }
        finally {
            if (timer !== undefined)
                clearTimeout(timer);
        }
    }
}
//# sourceMappingURL=task-registry.js.map