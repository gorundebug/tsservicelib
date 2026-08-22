import { RuntimeConfig } from "./runtime-config.js";
import type { CanonicalConfig } from "./types.js";
import type { Context } from "../context.js";
import { type Logger } from "../environment/log.js";
import type { Metrics } from "../environment/metrics/index.js";
import type { Lifecycle } from "../lifecycle.js";
type MutableRecord = Record<string, unknown>;
export interface ConfigSchema<T extends CanonicalConfig> {
    parse(value: unknown): T;
}
export interface EnvironmentPatch {
    readonly environment: string;
    readonly path: readonly string[];
    parse(value: string): unknown;
}
export interface ConfigLoadOptions<T extends CanonicalConfig> {
    readonly configPath: string;
    readonly valuesPath?: string;
    readonly defaults?: Readonly<Record<string, unknown>>;
    readonly environment?: Readonly<Record<string, string | undefined>>;
    readonly patches?: readonly EnvironmentPatch[];
    readonly schema: ConfigSchema<T>;
}
export interface RuntimeConfigReloadSource<T extends CanonicalConfig> {
    readonly valuesPath: string;
    readonly pollIntervalMs?: number | undefined;
    load(): Promise<RuntimeConfig<T>>;
}
export interface RuntimeConfigLoaderOptions<T extends CanonicalConfig> extends RuntimeConfigReloadSource<T> {
    readonly store: RuntimeConfigStore<T>;
    readonly service: string;
    readonly metrics: Metrics;
    readonly logger: Logger;
}
export declare function deepMerge(base: Readonly<Record<string, unknown>>, overlay: Readonly<Record<string, unknown>>): MutableRecord;
export declare function applyEnvironment(value: MutableRecord, patches: readonly EnvironmentPatch[], environment: Readonly<Record<string, string | undefined>>): void;
export declare function loadRuntimeConfig<T extends CanonicalConfig>(options: ConfigLoadOptions<T>): Promise<RuntimeConfig<T>>;
export declare class RuntimeConfigStore<T extends CanonicalConfig> {
    #private;
    constructor(initial: RuntimeConfig<T>);
    current(): RuntimeConfig<T>;
    /** Registers a synchronous observer for successfully published snapshots. */
    subscribe(listener: (config: RuntimeConfig<T>) => void): () => void;
    /** Registers validation that must pass before a candidate becomes observable. */
    validate(validator: (config: RuntimeConfig<T>, current: RuntimeConfig<T>) => void): () => void;
    reload(load: () => Promise<RuntimeConfig<T>>, onPublished?: (config: RuntimeConfig<T>) => void, onRejected?: (error: unknown) => void): Promise<boolean>;
    publish(config: RuntimeConfig<T>): void;
}
/** Polls the mutable values file and atomically publishes only stable, valid snapshots. */
export declare class RuntimeConfigLoader<T extends CanonicalConfig> implements Lifecycle {
    #private;
    constructor(options: RuntimeConfigLoaderOptions<T>);
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private synchronizeInitialSnapshot;
    private poll;
}
export {};
//# sourceMappingURL=loader.d.ts.map