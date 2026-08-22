import { DuplicateKeyError, StoreAlreadyStartedError, StoreStoppedError } from "./storage.js";
const SHRINK_FACTOR = 4;
const SHARD_COUNT = 64;
const DEFAULT_MIN_CAPACITY = 1_000;
/**
 * Two-generation, sharded pending-request map. Rotation reclaims bucket
 * capacity after a burst; it never expires or drops a live entry.
 */
export class RotatingMap {
    #intervalMs;
    #minCapacity;
    #shards;
    #objectIds = new WeakMap();
    #nextObjectId = 1;
    #timer;
    #started = false;
    #stopped = false;
    constructor(intervalMs, minCapacity = DEFAULT_MIN_CAPACITY) {
        if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
            throw new RangeError("rotating map interval must be positive");
        }
        if (!Number.isSafeInteger(minCapacity) || minCapacity < 0) {
            throw new RangeError("rotating map minimum capacity must be a non-negative integer");
        }
        this.#intervalMs = intervalMs;
        this.#minCapacity = minCapacity;
        this.#shards = Array.from({ length: SHARD_COUNT }, () => ({
            current: new Map(),
            previous: new Map(),
            highWaterMark: 0
        }));
    }
    start(context) {
        void context;
        if (this.#stopped) {
            throw new StoreStoppedError();
        }
        if (this.#started) {
            throw new StoreAlreadyStartedError();
        }
        this.#started = true;
        this.armTimer();
    }
    stop(context) {
        void context;
        if (this.#stopped) {
            return;
        }
        this.#stopped = true;
        if (this.#timer !== undefined) {
            clearTimeout(this.#timer);
            this.#timer = undefined;
        }
    }
    size() {
        let size = 0;
        for (const shard of this.#shards) {
            size += shard.current.size + shard.previous.size;
        }
        return size;
    }
    set(key, value) {
        const shard = this.shard(key);
        if (shard.current.has(key) || shard.previous.has(key)) {
            throw new DuplicateKeyError(key);
        }
        shard.current.set(key, { value });
    }
    getOrCreate(key, factory) {
        const shard = this.shard(key);
        const current = shard.current.get(key);
        if (current !== undefined) {
            return [current.value, true];
        }
        const previous = shard.previous.get(key);
        if (previous !== undefined) {
            return [previous.value, true];
        }
        const value = factory();
        shard.current.set(key, { value });
        return [value, false];
    }
    get(key) {
        const shard = this.shard(key);
        const current = shard.current.get(key);
        if (current !== undefined) {
            return [current.value, true];
        }
        const previous = shard.previous.get(key);
        if (previous !== undefined) {
            return [previous.value, true];
        }
        return [undefined, false];
    }
    pop(key) {
        const shard = this.shard(key);
        const current = shard.current.get(key);
        if (current !== undefined) {
            shard.current.delete(key);
            return [current.value, true];
        }
        const previous = shard.previous.get(key);
        if (previous !== undefined) {
            shard.previous.delete(key);
            return [previous.value, true];
        }
        return [undefined, false];
    }
    rotate() {
        for (const shard of this.#shards) {
            this.rotateShard(shard);
        }
    }
    rotateShard(shard) {
        const total = shard.current.size + shard.previous.size;
        const shouldRotate = shard.highWaterMark === 0 || total * SHRINK_FACTOR < shard.highWaterMark;
        shard.highWaterMark = Math.max(shard.highWaterMark, total);
        if (shard.highWaterMark < this.#minCapacity || !shouldRotate) {
            return;
        }
        const combined = new Map(shard.current);
        for (const [key, value] of shard.previous) {
            if (!combined.has(key)) {
                combined.set(key, value);
            }
        }
        shard.current = new Map();
        shard.previous = combined;
        shard.highWaterMark = total;
    }
    armTimer() {
        this.#timer = setTimeout(() => {
            if (this.#stopped) {
                return;
            }
            this.rotate();
            this.armTimer();
        }, this.#intervalMs);
        this.#timer.unref();
    }
    shard(key) {
        const shard = this.#shards[this.hash(key) & (SHARD_COUNT - 1)];
        if (shard === undefined) {
            throw new Error("rotating map shard index is outside the fixed shard table");
        }
        return shard;
    }
    hash(key) {
        const type = typeof key;
        if ((type === "object" && key !== null) || type === "function") {
            // The runtime type check proves the WeakMap-key boundary that generic K
            // cannot express to TypeScript's control-flow analyser.
            const object = key;
            let id = this.#objectIds.get(object);
            if (id === undefined) {
                id = this.#nextObjectId++;
                this.#objectIds.set(object, id);
            }
            return mix(id);
        }
        return hashString(`${type}:${String(key)}`);
    }
}
function hashString(value) {
    let hash = 0x811c9dc5;
    for (let index = 0; index < value.length; index += 1) {
        hash ^= value.charCodeAt(index);
        hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
}
function mix(value) {
    let hash = value | 0;
    hash = Math.imul(hash ^ (hash >>> 16), 0x45d9f3b);
    hash = Math.imul(hash ^ (hash >>> 16), 0x45d9f3b);
    return (hash ^ (hash >>> 16)) >>> 0;
}
//# sourceMappingURL=rotating-map.js.map