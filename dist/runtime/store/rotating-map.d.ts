import type { Context } from "../context.js";
export type MapLookup<V> = readonly [value: V | undefined, found: boolean];
export type MapCreation<V> = readonly [value: V, loaded: boolean];
/**
 * Two-generation, sharded pending-request map. Rotation reclaims bucket
 * capacity after a burst; it never expires or drops a live entry.
 */
export declare class RotatingMap<K, V> {
    #private;
    constructor(intervalMs: number, minCapacity?: number);
    start(context: Context): void;
    stop(context: Context): void;
    size(): number;
    set(key: K, value: V): void;
    getOrCreate(key: K, factory: () => V): MapCreation<V>;
    get(key: K): MapLookup<V>;
    pop(key: K): MapLookup<V>;
    protected rotate(): void;
    private rotateShard;
    private armTimer;
    private shard;
    private hash;
}
//# sourceMappingURL=rotating-map.d.ts.map