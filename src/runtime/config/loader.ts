import { readFile } from "node:fs/promises";

import { parse } from "yaml";

import { RuntimeConfig } from "./runtime-config.js";
import { deepFreeze } from "./immutable.js";
import type { CanonicalConfig } from "./types.js";
import type { Context } from "../context.js";
import { err, type Logger } from "../environment/log.js";
import type { Int64Counter, Metrics } from "../environment/metrics/index.js";
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

export interface RuntimeConfigLoaderOptions<
  T extends CanonicalConfig
> extends RuntimeConfigReloadSource<T> {
  readonly store: RuntimeConfigStore<T>;
  readonly service: string;
  readonly metrics: Metrics;
  readonly logger: Logger;
}

function isRecord(value: unknown): value is MutableRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, source: string): MutableRecord {
  if (!isRecord(value)) {
    throw new Error(`${source} root must be a mapping`);
  }
  return value;
}

export function deepMerge(
  base: Readonly<Record<string, unknown>>,
  overlay: Readonly<Record<string, unknown>>
): MutableRecord {
  const result: MutableRecord = Object.fromEntries(
    Object.entries(base).map(([key, value]) => [key, cloneConfigValue(value)])
  );
  for (const [key, value] of Object.entries(overlay)) {
    const current = result[key];
    result[key] =
      isRecord(current) && isRecord(value) ? deepMerge(current, value) : cloneConfigValue(value);
  }
  return result;
}

function cloneConfigValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(cloneConfigValue);
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [key, cloneConfigValue(child)])
    );
  }
  return value;
}

function setPath(root: MutableRecord, path: readonly string[], value: unknown): void {
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

export function applyEnvironment(
  value: MutableRecord,
  patches: readonly EnvironmentPatch[],
  environment: Readonly<Record<string, string | undefined>>
): void {
  for (const patch of patches) {
    const raw = environment[patch.environment];
    if (raw !== undefined) {
      setPath(value, patch.path, patch.parse(raw));
    }
  }
}

async function readYaml(path: string): Promise<MutableRecord> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error: unknown) {
    throw new Error(`cannot read config file ${path}`, { cause: error });
  }
  let value: unknown;
  try {
    value = parse(text) as unknown;
  } catch (error: unknown) {
    throw new Error(`cannot parse config file ${path}`, { cause: error });
  }
  return requireRecord(value, path);
}

export async function loadRuntimeConfig<T extends CanonicalConfig>(
  options: ConfigLoadOptions<T>
): Promise<RuntimeConfig<T>> {
  const base = await readYaml(options.configPath);
  const values = options.valuesPath === undefined ? {} : await readYaml(options.valuesPath);
  const merged = deepMerge(deepMerge(options.defaults ?? {}, base), values);
  applyEnvironment(merged, options.patches ?? [], options.environment ?? process.env);
  const parsed = deepFreeze(options.schema.parse(merged));
  return new RuntimeConfig(parsed);
}

export class RuntimeConfigStore<T extends CanonicalConfig> {
  #current: RuntimeConfig<T>;
  readonly #validators = new Set<(config: RuntimeConfig<T>, current: RuntimeConfig<T>) => void>();
  readonly #listeners = new Set<(config: RuntimeConfig<T>) => void>();

  public constructor(initial: RuntimeConfig<T>) {
    this.#current = initial;
  }

  public current(): RuntimeConfig<T> {
    return this.#current;
  }

  /** Registers a synchronous observer for successfully published snapshots. */
  public subscribe(listener: (config: RuntimeConfig<T>) => void): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  /** Registers validation that must pass before a candidate becomes observable. */
  public validate(
    validator: (config: RuntimeConfig<T>, current: RuntimeConfig<T>) => void
  ): () => void {
    this.#validators.add(validator);
    return () => {
      this.#validators.delete(validator);
    };
  }

  public async reload(
    load: () => Promise<RuntimeConfig<T>>,
    onPublished: (config: RuntimeConfig<T>) => void = () => undefined,
    onRejected: (error: unknown) => void = () => undefined
  ): Promise<boolean> {
    try {
      const candidate = await load();
      this.publish(candidate);
      onPublished(candidate);
    } catch (error: unknown) {
      onRejected(error);
      return false;
    }
    return true;
  }

  public publish(config: RuntimeConfig<T>): void {
    for (const validator of this.#validators) validator(config, this.#current);
    this.#current = config;
    for (const listener of this.#listeners) listener(config);
  }
}

/** Polls the mutable values file and atomically publishes only stable, valid snapshots. */
export class RuntimeConfigLoader<T extends CanonicalConfig> implements Lifecycle {
  readonly #source: RuntimeConfigReloadSource<T>;
  readonly #store: RuntimeConfigStore<T>;
  readonly #logger: Logger;
  readonly #success: Int64Counter;
  readonly #error: Int64Counter;
  readonly #pollIntervalMs: number;
  #observed: Buffer | undefined;
  #timer: NodeJS.Timeout | undefined;
  #polling: Promise<void> | undefined;
  #readFailureRecorded = false;
  #state: "created" | "running" | "stopped" = "created";

  public constructor(options: RuntimeConfigLoaderOptions<T>) {
    if (
      !Number.isSafeInteger(options.pollIntervalMs ?? 250) ||
      (options.pollIntervalMs ?? 250) < 1
    ) {
      throw new RangeError("config reload poll interval must be a positive integer");
    }
    this.#source = options;
    this.#store = options.store;
    this.#logger = options.logger;
    this.#pollIntervalMs = options.pollIntervalMs ?? 250;
    const scope = options.metrics.scope("service", { service: options.service });
    this.#success = scope.counter(
      "config_reloads_total",
      "Total number of config reload attempts",
      { event: "success" }
    );
    this.#error = scope.counter("config_reloads_total", "Total number of config reload attempts", {
      event: "error"
    });
  }

  public async start(context: Context): Promise<void> {
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

  public async stop(context: Context): Promise<void> {
    void context;
    if (this.#state === "stopped") return;
    this.#state = "stopped";
    if (this.#timer !== undefined) {
      clearInterval(this.#timer);
      this.#timer = undefined;
    }
    await this.#polling;
  }

  private async synchronizeInitialSnapshot(): Promise<void> {
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

  private async poll(context: Context): Promise<void> {
    if (this.#state !== "running") return;
    let before: Buffer;
    try {
      before = await readFile(this.#source.valuesPath);
      this.#readFailureRecorded = false;
    } catch (error: unknown) {
      if (!this.#readFailureRecorded) {
        this.#readFailureRecorded = true;
        this.#error.inc(context);
        this.#logger.error(context, "error reading override config file", asErrorField(error));
      }
      return;
    }
    if (this.#observed?.equals(before) === true) return;

    try {
      const candidate = await this.#source.load();
      const after = await readFile(this.#source.valuesPath);
      if (!before.equals(after)) return;
      this.#store.publish(candidate);
      this.#observed = after;
      this.#success.inc(context);
    } catch (error: unknown) {
      this.#observed = before;
      this.#error.inc(context);
      this.#logger.error(context, "config reload error", asErrorField(error));
    }
  }
}

function asErrorField(value: unknown): ReturnType<typeof err> {
  return err(value instanceof Error ? value : new Error(String(value)));
}
