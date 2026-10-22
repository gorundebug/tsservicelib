import { Context } from "../context.js";
import { isTaskCancellation, reportUnhandledTaskError } from "../errors.js";
import { StoreAlreadyStartedError, StoreStoppedError } from "./storage.js";
const SHRINK_FACTOR = 4;
/** Event-loop implementation of Go's two-generation HashMapJoinStorage. */
export class HashMapJoinStorage {
    #config;
    #current = new Map();
    #previous = new Map();
    #metricsEnabled;
    #count;
    #evictionsTotal;
    #highWaterMark = 0;
    #rotationTimer;
    #rotationContext;
    #started = false;
    #stopped = false;
    #activeTTLJoins = 0;
    #stopCompletion;
    #releaseStop;
    constructor(environment, config) {
        this.#config = config;
        const metrics = environment.metrics();
        this.#metricsEnabled = metrics.enabled();
        const scope = metrics.scope("hashmap_join_storage", {
            service: environment.serviceConfig().name,
            name: config.name()
        });
        this.#count = scope.gauge("count", "Elements count stored in a join storage");
        this.#evictionsTotal = scope.counter("evictions_total", "Total number of items evicted from join storage by TTL");
        if (this.#metricsEnabled)
            this.#count.set(0);
    }
    size() {
        return this.#current.size + this.#previous.size;
    }
    start(context) {
        if (this.#stopped)
            throw new StoreStoppedError();
        if (this.#started)
            throw new StoreAlreadyStartedError();
        this.#started = true;
        this.#rotationContext = context;
        this.armRotation();
    }
    stop(context) {
        void context;
        if (this.#stopCompletion !== undefined)
            return this.#stopCompletion;
        this.#stopped = true;
        if (this.#rotationTimer !== undefined) {
            clearTimeout(this.#rotationTimer);
            this.#rotationTimer = undefined;
        }
        // Go's rotation write lock waits for TTL-bearing JoinValue readers,
        // not for expiry callbacks or zero-TTL calls. Stop ends maintenance;
        // individual groups and their accepted expiry callbacks remain alive.
        this.#stopCompletion =
            this.#activeTTLJoins === 0
                ? Promise.resolve()
                : new Promise((resolve) => {
                    this.#releaseStop = resolve;
                });
        return this.#stopCompletion;
    }
    async joinValue(context, key, index, value, callback) {
        if (!Number.isSafeInteger(index) || index < 0) {
            throw new RangeError(`join value index must be a non-negative integer, got ${String(index)}`);
        }
        const ttl = this.effectiveTTL(context);
        const renewTTL = this.#config.renewTTL();
        if (ttl > 0) {
            if (this.#stopCompletion !== undefined)
                await this.#stopCompletion;
            this.#activeTTLJoins += 1;
        }
        try {
            for (;;) {
                const located = this.findLive(key);
                const item = located?.item ?? this.createItem(context, key, index, callback, ttl);
                const operation = item.tail.then(async () => {
                    const currentLocation = this.locate(key, item);
                    if (item.processed || this.expired(item) || currentLocation === undefined)
                        return false;
                    while (item.values.length <= index)
                        item.values.push([]);
                    item.values[index]?.push(value);
                    item.processed = await callback(item.values);
                    const locationAfterCallback = this.locate(key, item);
                    if (locationAfterCallback === undefined)
                        return true;
                    if (item.processed) {
                        item.cancelDeadline();
                        this.removeAt(key, item, locationAfterCallback);
                    }
                    else if (renewTTL && ttl > 0) {
                        if (locationAfterCallback === "previous")
                            this.#previous.delete(key);
                        item.deadline = Date.now() + ttl;
                        this.#current.set(key, item);
                        item.cancelDeadline();
                        this.armDeadline(context, key, item, ttl);
                    }
                    return true;
                });
                item.tail = operation.then(() => undefined, () => undefined);
                if (await operation)
                    return;
            }
        }
        finally {
            if (ttl > 0) {
                this.#activeTTLJoins -= 1;
                if (this.#activeTTLJoins === 0) {
                    this.#releaseStop?.();
                    this.#releaseStop = undefined;
                }
            }
        }
    }
    effectiveTTL(context) {
        return context.remainingMs() ?? this.#config.ttlMs();
    }
    expired(item) {
        return item.deadline !== undefined && item.deadline <= Date.now();
    }
    findLive(key) {
        const current = this.#current.get(key);
        if (current !== undefined)
            return this.expired(current) ? undefined : { item: current };
        const previous = this.#previous.get(key);
        if (previous !== undefined && !this.expired(previous))
            return { item: previous };
        return undefined;
    }
    createItem(context, key, index, callback, ttl) {
        const item = {
            values: Array.from({ length: index + 1 }, () => []),
            deadlineCallback: callback,
            deadline: ttl > 0 ? Date.now() + ttl : undefined,
            processed: false,
            cancelDeadline: () => undefined,
            tail: Promise.resolve()
        };
        const replaced = this.#current.get(key);
        if (this.#previous.delete(key) && this.#metricsEnabled)
            this.#count.dec();
        this.#current.set(key, item);
        if (this.#metricsEnabled && replaced === undefined)
            this.#count.inc();
        if (ttl > 0)
            this.armDeadline(context, key, item, ttl);
        return item;
    }
    armDeadline(context, key, item, ttl) {
        let retired = false;
        const expire = () => {
            if (retired)
                return;
            item.cancelDeadline();
            const operation = item.tail.then(async () => {
                if (item.processed)
                    return;
                // An admitted callback may renew the item while its old timer waits.
                // A context deadline is absolute; only the configured TTL can renew.
                if (context.deadline() === undefined && item.deadline !== undefined) {
                    const remaining = item.deadline - Date.now();
                    if (remaining > 0) {
                        item.cancelDeadline();
                        this.armDeadline(context, key, item, remaining);
                        return;
                    }
                }
                item.processed = true;
                try {
                    await item.deadlineCallback(item.values);
                }
                finally {
                    const location = this.locate(key, item);
                    if (location !== undefined)
                        this.removeAt(key, item, location, context);
                }
            });
            item.tail = operation.catch((error) => {
                if (!isTaskCancellation(error, context.signal()))
                    reportUnhandledTaskError(error);
            });
        };
        if (context.deadline() !== undefined) {
            const signal = context.signal();
            const aborted = () => {
                expire();
            };
            signal.addEventListener("abort", aborted, { once: true });
            item.cancelDeadline = () => {
                if (retired)
                    return;
                retired = true;
                signal.removeEventListener("abort", aborted);
            };
            if (signal.aborted)
                expire();
            return;
        }
        const timer = setTimeout(expire, Math.max(0, Math.ceil(ttl)));
        if (typeof timer === "object" && "unref" in timer)
            timer.unref();
        item.cancelDeadline = () => {
            if (retired)
                return;
            retired = true;
            clearTimeout(timer);
        };
    }
    locate(key, item) {
        if (this.#current.get(key) === item)
            return "current";
        if (this.#previous.get(key) === item)
            return "previous";
        return undefined;
    }
    removeAt(key, item, location, evictionContext) {
        const storage = location === "current" ? this.#current : this.#previous;
        if (storage.get(key) !== item)
            return;
        storage.delete(key);
        if (this.#metricsEnabled) {
            this.#count.dec();
            if (evictionContext !== undefined)
                this.#evictionsTotal.inc(evictionContext);
        }
    }
    armRotation() {
        const ttl = this.#config.ttlMs();
        if (this.#stopped || ttl <= 0)
            return;
        this.#rotationTimer = setTimeout(() => {
            this.rotate();
            this.armRotation();
        }, Math.max(1, Math.ceil(ttl)));
        this.#rotationTimer.unref();
    }
    rotate() {
        const total = this.#current.size + this.#previous.size;
        const shouldRotate = this.#highWaterMark === 0 || total * SHRINK_FACTOR < this.#highWaterMark;
        if (total > this.#highWaterMark)
            this.#highWaterMark = total;
        if (!shouldRotate)
            return;
        this.#highWaterMark = total;
        let rescued = 0;
        for (const [key, item] of this.#previous) {
            if (!this.#current.has(key)) {
                this.#current.set(key, item);
                rescued += 1;
            }
        }
        const evicted = this.#previous.size - rescued;
        this.#previous.clear();
        for (const [key, item] of this.#current)
            this.#previous.set(key, item);
        this.#current.clear();
        if (this.#metricsEnabled && evicted > 0) {
            this.#count.sub(evicted);
            this.#evictionsTotal.add(this.#rotationContext ?? Context.background(), evicted);
        }
    }
}
//# sourceMappingURL=hash-map-join-storage.js.map