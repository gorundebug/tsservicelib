import { readFile } from "node:fs/promises";
import { parse } from "yaml";
import { RuntimeConfig } from "./runtime-config.js";
import { deepFreeze } from "./immutable.js";
import { err } from "../environment/log.js";
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function requireRecord(value, source) {
    if (!isRecord(value)) {
        throw new Error(`${source} root must be a mapping`);
    }
    return value;
}
export function deepMerge(base, overlay) {
    const result = Object.fromEntries(Object.entries(base).map(([key, value]) => [key, cloneConfigValue(value)]));
    for (const [key, value] of Object.entries(overlay)) {
        const current = result[key];
        result[key] =
            isRecord(current) && isRecord(value) ? deepMerge(current, value) : cloneConfigValue(value);
    }
    return result;
}
function cloneConfigValue(value) {
    if (Array.isArray(value))
        return value.map(cloneConfigValue);
    if (isRecord(value)) {
        return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, cloneConfigValue(child)]));
    }
    return value;
}
function setPath(root, path, value) {
    if (path.length === 0) {
        throw new Error("environment patch path must not be empty");
    }
    let current = root;
    for (const segment of path.slice(0, -1)) {
        const child = current[segment];
        if (!isRecord(child)) {
            throw new Error(`environment patch path ${path.join(".")} does not exist`);
        }
        current = child;
    }
    const leaf = path.at(-1);
    if (leaf === undefined || !(leaf in current)) {
        throw new Error(`environment patch path ${path.join(".")} does not exist`);
    }
    current[leaf] = value;
}
export function applyEnvironment(value, patches, environment) {
    for (const patch of patches) {
        const raw = environment[patch.environment];
        if (raw !== undefined) {
            setPath(value, patch.path, patch.parse(raw));
        }
    }
}
async function readYaml(path) {
    let text;
    try {
        text = await readFile(path, "utf8");
    }
    catch (error) {
        throw new Error(`cannot read config file ${path}`, { cause: error });
    }
    let value;
    try {
        value = parse(text);
    }
    catch (error) {
        throw new Error(`cannot parse config file ${path}`, { cause: error });
    }
    return requireRecord(value, path);
}
export async function loadRuntimeConfig(options) {
    const base = await readYaml(options.configPath);
    const values = options.valuesPath === undefined ? {} : await readYaml(options.valuesPath);
    const overrides = options.overridesPath === undefined ? {} : await readYaml(options.overridesPath);
    const merged = deepMerge(deepMerge(deepMerge(options.defaults ?? {}, base), values), overrides);
    applyEnvironment(merged, options.patches ?? [], options.environment ?? process.env);
    const parsed = deepFreeze(options.schema.parse(merged));
    return new RuntimeConfig(parsed);
}
export class RuntimeConfigStore {
    #current;
    #validators = new Set();
    #listeners = new Set();
    constructor(initial) {
        this.#current = initial;
    }
    current() {
        return this.#current;
    }
    /** Registers a synchronous observer for successfully published snapshots. */
    subscribe(listener) {
        this.#listeners.add(listener);
        return () => {
            this.#listeners.delete(listener);
        };
    }
    /** Registers validation that must pass before a candidate becomes observable. */
    validate(validator) {
        this.#validators.add(validator);
        return () => {
            this.#validators.delete(validator);
        };
    }
    async reload(load, onPublished = () => undefined, onRejected = () => undefined) {
        try {
            const candidate = await load();
            this.publish(candidate);
            onPublished(candidate);
        }
        catch (error) {
            onRejected(error);
            return false;
        }
        return true;
    }
    publish(config) {
        for (const validator of this.#validators)
            validator(config, this.#current);
        this.#current = config;
        for (const listener of this.#listeners)
            listener(config);
    }
}
/** Polls the mutable values file and atomically publishes only stable, valid snapshots. */
export class RuntimeConfigLoader {
    #source;
    #store;
    #logger;
    #success;
    #error;
    #pollIntervalMs;
    #observed;
    #timer;
    #polling;
    #readFailureRecorded = false;
    #state = "created";
    constructor(options) {
        if (!Number.isSafeInteger(options.pollIntervalMs ?? 250) ||
            (options.pollIntervalMs ?? 250) < 1) {
            throw new RangeError("config reload poll interval must be a positive integer");
        }
        this.#source = options;
        this.#store = options.store;
        this.#logger = options.logger;
        this.#pollIntervalMs = options.pollIntervalMs ?? 250;
        const scope = options.metrics.scope("service", { service: options.service });
        this.#success = scope.counter("config_reloads_total", "Total number of config reload attempts", { event: "success" });
        this.#error = scope.counter("config_reloads_total", "Total number of config reload attempts", {
            event: "error"
        });
    }
    async start(context) {
        if (this.#state !== "created") {
            throw new Error(`config loader cannot start from state ${this.#state}`);
        }
        await this.synchronizeInitialSnapshot();
        this.#state = "running";
        this.#timer = setInterval(() => {
            if (this.#polling === undefined) {
                this.#polling = this.poll(context).finally(() => {
                    this.#polling = undefined;
                });
            }
        }, this.#pollIntervalMs);
        this.#timer.unref();
    }
    async stop(context) {
        void context;
        if (this.#state === "stopped")
            return;
        this.#state = "stopped";
        if (this.#timer !== undefined) {
            clearInterval(this.#timer);
            this.#timer = undefined;
        }
        await this.#polling;
    }
    async synchronizeInitialSnapshot() {
        for (;;) {
            const before = await readFile(this.#source.valuesPath);
            const candidate = await this.#source.load();
            const after = await readFile(this.#source.valuesPath);
            if (before.equals(after)) {
                this.#store.publish(candidate);
                this.#observed = after;
                return;
            }
        }
    }
    async poll(context) {
        if (this.#state !== "running")
            return;
        let before;
        try {
            before = await readFile(this.#source.valuesPath);
            this.#readFailureRecorded = false;
        }
        catch (error) {
            if (!this.#readFailureRecorded) {
                this.#readFailureRecorded = true;
                this.#error.inc(context);
                this.#logger.error(context, "error reading override config file", asErrorField(error));
            }
            return;
        }
        if (this.#observed?.equals(before) === true)
            return;
        try {
            const candidate = await this.#source.load();
            const after = await readFile(this.#source.valuesPath);
            if (!before.equals(after))
                return;
            this.#store.publish(candidate);
            this.#observed = after;
            this.#success.inc(context);
        }
        catch (error) {
            this.#observed = before;
            this.#error.inc(context);
            this.#logger.error(context, "config reload error", asErrorField(error));
        }
    }
}
function asErrorField(value) {
    return err(value instanceof Error ? value : new Error(String(value)));
}
//# sourceMappingURL=loader.js.map