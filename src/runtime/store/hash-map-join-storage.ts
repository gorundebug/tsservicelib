import { Context, type MessageContext } from "../context.js";
import type { Int64Counter, Int64Gauge } from "../environment/index.js";
import type { RuntimeEnvironment } from "../environment/runtime-environment.js";
import type {
  JoinStorage,
  JoinStorageConfig,
  JoinValueCallback,
  JoinValues
} from "./join-storage.js";
import { StoreAlreadyStartedError, StoreNotStartedError, StoreStoppedError } from "./storage.js";

const SHRINK_FACTOR = 4;

interface Item {
  readonly values: JoinValues;
  readonly deadlineCallback: JoinValueCallback;
  deadline: number | undefined;
  processed: boolean;
  cancelDeadline(): void;
  tail: Promise<void>;
}

/** Event-loop implementation of Go's two-generation HashMapJoinStorage. */
export class HashMapJoinStorage<K> implements JoinStorage<K> {
  readonly #config: JoinStorageConfig;
  readonly #current = new Map<K, Item>();
  readonly #previous = new Map<K, Item>();
  readonly #count: Int64Gauge;
  readonly #evictionsTotal: Int64Counter;
  #highWaterMark = 0;
  #rotationTimer: NodeJS.Timeout | undefined;
  #rotationContext: Context | undefined;
  #started = false;
  #stopped = false;

  public constructor(environment: RuntimeEnvironment, config: JoinStorageConfig) {
    this.#config = config;
    const scope = environment.metrics().scope("hashmap_join_storage", {
      service: environment.serviceConfig().name,
      name: config.name()
    });
    this.#count = scope.gauge("count", "Elements count stored in a join storage");
    this.#evictionsTotal = scope.counter(
      "evictions_total",
      "Total number of items evicted from join storage by TTL"
    );
    this.#count.set(0);
  }

  public size(): number {
    return this.#current.size + this.#previous.size;
  }

  public start(context: Context): void {
    if (this.#stopped) throw new StoreStoppedError();
    if (this.#started) throw new StoreAlreadyStartedError();
    this.#started = true;
    this.#rotationContext = context;
    this.armRotation();
  }

  public async stop(context: Context): Promise<void> {
    void context;
    if (this.#stopped) return;
    this.#stopped = true;
    if (this.#rotationTimer !== undefined) {
      clearTimeout(this.#rotationTimer);
      this.#rotationTimer = undefined;
    }
    const items = new Set([...this.#current.values(), ...this.#previous.values()]);
    for (const item of items) item.cancelDeadline();
    await Promise.allSettled([...items].map((item) => item.tail));
    this.#current.clear();
    this.#previous.clear();
    this.#count.set(0);
  }

  public async joinValue(
    context: MessageContext,
    key: K,
    index: number,
    value: unknown,
    callback: JoinValueCallback
  ): Promise<void> {
    if (!Number.isSafeInteger(index) || index < 0) {
      throw new RangeError(`join value index must be a non-negative integer, got ${String(index)}`);
    }

    const ttl = this.effectiveTTL(context);
    for (;;) {
      if (this.#stopped) throw new StoreStoppedError();
      if (!this.#started) throw new StoreNotStartedError();
      const located = this.findLive(key);
      const item = located?.item ?? this.createItem(context, key, index, callback, ttl);
      const operation = item.tail.then(async () => {
        const currentLocation = this.locate(key, item);
        if (item.processed || this.expired(item) || currentLocation === undefined) return false;
        while (item.values.length <= index) item.values.push([]);
        item.values[index]?.push(value);
        item.processed = await callback(item.values);
        if (item.processed) {
          item.cancelDeadline();
          this.removeAt(key, item, currentLocation);
        } else if (this.#config.renewTTL() && ttl > 0) {
          if (currentLocation === "previous") this.#previous.delete(key);
          item.deadline = Date.now() + ttl;
          this.#current.set(key, item);
          item.cancelDeadline();
          this.armDeadline(context, key, item, ttl);
        }
        return true;
      });
      item.tail = operation.then(
        () => undefined,
        () => undefined
      );
      if (await operation) return;
    }
  }

  private effectiveTTL(context: MessageContext): number {
    return context.remainingMs() ?? this.#config.ttlMs();
  }

  private expired(item: Item): boolean {
    return item.deadline !== undefined && item.deadline <= Date.now();
  }

  private findLive(key: K): { readonly item: Item } | undefined {
    const current = this.#current.get(key);
    if (current !== undefined) return this.expired(current) ? undefined : { item: current };
    const previous = this.#previous.get(key);
    if (previous !== undefined && !this.expired(previous)) return { item: previous };
    return undefined;
  }

  private createItem(
    context: MessageContext,
    key: K,
    index: number,
    callback: JoinValueCallback,
    ttl: number
  ): Item {
    const item: Item = {
      values: Array.from({ length: index + 1 }, () => []),
      deadlineCallback: callback,
      deadline: ttl > 0 ? Date.now() + ttl : undefined,
      processed: false,
      cancelDeadline: () => undefined,
      tail: Promise.resolve()
    };
    const replaced = this.#current.get(key);
    this.#current.set(key, item);
    if (replaced === undefined) this.#count.inc();
    if (ttl > 0) this.armDeadline(context, key, item, ttl);
    return item;
  }

  private armDeadline(context: MessageContext, key: K, item: Item, ttl: number): void {
    let retired = false;
    const expire = (): void => {
      if (retired) return;
      item.cancelDeadline();
      const operation = item.tail.then(async () => {
        if (item.processed) return;
        item.processed = true;
        await item.deadlineCallback(item.values);
        const location = this.locate(key, item);
        if (location !== undefined) this.removeAt(key, item, location, context);
      });
      item.tail = operation.catch(() => undefined);
    };

    if (context.deadline() !== undefined) {
      const signal = context.signal();
      const aborted = (): void => {
        expire();
      };
      signal.addEventListener("abort", aborted, { once: true });
      item.cancelDeadline = () => {
        if (retired) return;
        retired = true;
        signal.removeEventListener("abort", aborted);
      };
      if (signal.aborted) expire();
      return;
    }

    const signal = context.signal();
    const aborted = (): void => {
      expire();
    };
    signal.addEventListener("abort", aborted, { once: true });
    const timer = setTimeout(expire, Math.max(0, Math.ceil(ttl)));
    if (typeof timer === "object" && "unref" in timer) timer.unref();
    item.cancelDeadline = () => {
      if (retired) return;
      retired = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", aborted);
    };
    if (signal.aborted) expire();
  }

  private locate(key: K, item: Item): "current" | "previous" | undefined {
    if (this.#current.get(key) === item) return "current";
    if (this.#previous.get(key) === item) return "previous";
    return undefined;
  }

  private removeAt(
    key: K,
    item: Item,
    location: "current" | "previous",
    evictionContext?: Context
  ): void {
    const storage = location === "current" ? this.#current : this.#previous;
    if (storage.get(key) !== item) return;
    storage.delete(key);
    this.#count.dec();
    if (evictionContext !== undefined) this.#evictionsTotal.inc(evictionContext);
  }

  private armRotation(): void {
    const ttl = this.#config.ttlMs();
    if (this.#stopped || ttl <= 0) return;
    this.#rotationTimer = setTimeout(
      () => {
        this.rotate();
        this.armRotation();
      },
      Math.max(1, Math.ceil(ttl))
    );
    this.#rotationTimer.unref();
  }

  private rotate(): void {
    const total = this.#current.size + this.#previous.size;
    const shouldRotate = this.#highWaterMark === 0 || total * SHRINK_FACTOR < this.#highWaterMark;
    if (total > this.#highWaterMark) this.#highWaterMark = total;
    if (!shouldRotate) return;

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
    for (const [key, item] of this.#current) this.#previous.set(key, item);
    this.#current.clear();
    if (evicted > 0) {
      this.#count.sub(evicted);
      this.#evictionsTotal.add(this.#rotationContext ?? Context.background(), evicted);
    }
  }
}
