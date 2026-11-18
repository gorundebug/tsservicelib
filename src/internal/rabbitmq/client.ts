import { Worker } from "node:worker_threads";
import { serialize, deserialize } from "node:v8";
import {
  Context,
  type MessageContext,
  type RabbitMQDataConnectorConfig,
  type RabbitMQEndpointConfig
} from "../../runtime/index.js";
import { wait } from "./wait.js";
import {
  BYTE_LIMIT,
  MESSAGE_LIMIT,
  type Event,
  type Command,
  type Delivery,
  type NativeMessage
} from "./protocol.js";
export { wait, cancellation } from "./wait.js";
export { BYTE_LIMIT, MESSAGE_LIMIT, type Delivery, type NativeMessage } from "./protocol.js";
export const PENDING_MESSAGES = 256;
export class Session {
  public worker: Worker | undefined;
  public ready = false;
  public epoch = 0;
  public stopped = false;
  public paused = false;
  public readonly limit: number;
  #pending = new Map<
    number,
    {
      resolve: (v: Delivery | undefined) => void;
      reject: (e: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  #serial = 0;
  #attempts = 0;
  #recovering: Promise<void> | undefined;
  #controller = new AbortController();
  #closing: Promise<void> | undefined;
  #removeOwner: (() => void) | undefined;
  public constructor(
    public readonly config: RabbitMQDataConnectorConfig,
    public readonly ep: RabbitMQEndpointConfig,
    public readonly source: boolean,
    private readonly report: (error: Error) => void,
    private readonly receive: (message: NativeMessage, id: number, epoch: number) => void = () =>
      undefined
  ) {
    this.limit = (source ? ep.consumerCredits : ep.publisherMaxInFlight) ?? 256;
  }
  public async start(context: Context): Promise<void> {
    const abort = (): void => {
      void this.close(new Context().bounded(1000));
    };
    context.signal().addEventListener("abort", abort, { once: true });
    this.#removeOwner = () => {
      context.signal().removeEventListener("abort", abort);
    };
    try {
      if (context.cancelled()) throw new Error("RabbitMQ startup context cancelled");
      await this.open(context);
    } catch (error) {
      await this.close(context);
      throw error;
    }
  }
  private open(context: Context): Promise<void> {
    if (this.stopped) return Promise.reject(new Error("RabbitMQ stopped"));
    this.epoch++;
    const epoch = this.epoch;
    const worker = new Worker(new URL("./worker.js", import.meta.url), {
      workerData: {
        config: this.config,
        endpoint: this.ep,
        source: this.source,
        server: this.#attempts
      },
      stdout: true,
      stderr: true
    });
    // SDK logs connection IDs; consume streams without retaining buffers.
    worker.stdout.on("data", () => undefined);
    worker.stderr.on("data", () => undefined);
    this.worker = worker;
    let started = false;
    let startupTimer: ReturnType<typeof setTimeout>;
    const opening = new Promise<void>((resolve, reject) => {
      startupTimer = setTimeout(() => {
        reject(new Error("RabbitMQ connection startup deadline exceeded"));
      }, this.config.connectTimeoutMs);
      const failure = (error: Error): void => {
        if (epoch !== this.epoch || this.stopped) return;
        if (!started) reject(error);
        else this.invalidate(error);
      };
      worker.on("error", () => {
        failure(new Error("RabbitMQ native worker failed"));
      });
      worker.on("exit", () => {
        failure(new Error("RabbitMQ native worker exited"));
      });
      worker.on("message", (event: Event) => {
        if (epoch !== this.epoch || this.stopped) return;
        if (event.kind === "connected") {
          clearTimeout(startupTimer);
          startupTimer = setTimeout(() => {
            reject(new Error("RabbitMQ topology startup deadline exceeded"));
          }, this.config.operationTimeoutMs);
        } else if (event.kind === "ready") {
          started = true;
          this.ready = true;
          resolve();
        } else if (event.kind === "error") failure(new Error(event.error));
        else if (event.kind === "message") {
          if (!this.paused)
            this.receive(deserialize(Buffer.from(event.bytes)) as NativeMessage, event.id, epoch);
        } else if (event.kind !== "closed") {
          const pending = this.#pending.get(event.id);
          if (!pending) return;
          this.#pending.delete(event.id);
          clearTimeout(pending.timer);
          if (event.kind === "settled" && event.error) pending.reject(new Error(event.error));
          else pending.resolve(event.kind === "delivery" ? event.delivery : undefined);
        }
      });
    });
    return wait(
      opening,
      context,
      this.config.connectTimeoutMs + this.config.operationTimeoutMs
    ).finally(() => {
      clearTimeout(startupTimer);
    });
  }
  public invalidate(error: Error): void {
    if (this.#recovering || this.stopped) return;
    this.ready = false;
    this.epoch++;
    for (const p of this.#pending.values()) {
      clearTimeout(p.timer);
      p.reject(error);
    }
    this.#pending.clear();
    this.report(error);
    const worker = this.worker;
    this.worker = undefined;
    this.#recovering = (async () => {
      await worker?.terminate();
      while (!this.stopped && !this.paused && this.#attempts++ < 60) {
        try {
          await wait(
            new Promise<void>((r) => setTimeout(r, 250)),
            new Context().withExternalCancellation(this.#controller.signal)
          );
          await this.open(new Context().withExternalCancellation(this.#controller.signal));
          return;
        } catch {
          this.ready = false;
          this.epoch++;
          const failed = this.worker;
          this.worker = undefined;
          await failed?.terminate();
        }
      }
      if (!this.stopped && !this.paused)
        this.report(new Error("RabbitMQ reconnect attempts exhausted"));
    })().finally(() => {
      this.#recovering = undefined;
    });
    void this.#recovering.catch(() => undefined);
  }
  private command(
    command:
      | Omit<Extract<Command, { kind: "publish" }>, "id">
      | Omit<Extract<Command, { kind: "settle" }>, "id">
  ): Promise<Delivery | undefined> {
    if (!this.ready || !this.worker || this.stopped)
      throw new Error("RabbitMQ connection unavailable");
    if (this.#pending.size >= this.limit)
      throw new Error("RabbitMQ native pending limit exhausted");
    const id = ++this.#serial;
    const promise = new Promise<Delivery | undefined>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.invalidate(new Error("RabbitMQ native deadline exceeded; delivery unknown"));
      }, this.config.operationTimeoutMs);
      this.#pending.set(id, { resolve, reject, timer });
    });
    this.worker.postMessage({ ...command, id });
    return promise;
  }
  public publish(encoded: Uint8Array, context: Context): Promise<Delivery> {
    const op = this.command({ kind: "publish", bytes: encoded });
    return wait(op, context, this.config.operationTimeoutMs).then(
      (v) => v ?? { outcome: "unknown" },
      (error: unknown) => {
        // Cancel/timeout must dispose native pending state before another admission.
        this.invalidate(new Error("RabbitMQ publication cancelled or timed out; outcome unknown"));
        return {
          outcome: "unknown",
          error: error instanceof Error ? error : new Error("RabbitMQ publication failed")
        };
      }
    );
  }
  public async settle(
    id: number,
    epoch: number,
    action: "accept" | "release" | "reject",
    context: Context
  ): Promise<void> {
    if (epoch !== this.epoch || !this.ready)
      throw new Error("RabbitMQ delivery belongs to expired connection");
    await wait(
      this.command({ kind: "settle", delivery: id, action }),
      context,
      this.config.operationTimeoutMs
    );
  }
  public pause(): void {
    this.paused = true;
    this.worker?.postMessage({ kind: "pause" } satisfies Command);
  }
  public close(context: Context): Promise<void> {
    if (this.#closing)
      return wait(this.#closing, context, this.config.operationTimeoutMs).catch(() => undefined);
    this.#closing = this.finishClose(context);
    return this.#closing;
  }
  private async finishClose(context: Context): Promise<void> {
    this.#removeOwner?.();
    this.#removeOwner = undefined;
    this.stopped = true;
    this.ready = false;
    this.epoch++;
    this.#controller.abort();
    for (const p of this.#pending.values()) {
      clearTimeout(p.timer);
      p.reject(new Error("RabbitMQ stopped; publication outcome unknown"));
    }
    this.#pending.clear();
    const worker = this.worker;
    if (worker) {
      let finish!: () => void;
      const closed = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const message = (event: Event): void => {
        if (event.kind === "closed") finish();
      };
      worker.on("message", message);
      worker.once("exit", finish);
      try {
        worker.postMessage({ kind: "close" } satisfies Command);
        await wait(closed, context, this.config.operationTimeoutMs);
      } catch {
        /* Native close exceeded its budget; termination below is mandatory. */
      } finally {
        worker.off("message", message);
        worker.off("exit", finish);
      }
    }
    this.worker = undefined;
    const cleanup = Promise.allSettled([worker?.terminate(), this.#recovering]);
    await wait(cleanup, context, this.config.operationTimeoutMs).catch(() => undefined);
  }
}
export function snapshot(message: NativeMessage, context: MessageContext): Uint8Array {
  if (message.to !== undefined)
    throw new Error("RabbitMQ destination cannot override modeled endpoint");
  const body: unknown = message.body;
  if (body !== undefined && typeof body !== "string" && !(body instanceof Uint8Array))
    throw new Error(
      "RabbitMQ JS worker profile requires bytes or string body; native codec objects cannot cross owners"
    );
  const copy = deserialize(
    serialize({ ...message, ...(body instanceof Uint8Array ? { body: Buffer.from(body) } : {}) })
  ) as NativeMessage;
  const properties = { ...copy.application_properties } as Record<string, unknown>;
  for (const [key, value] of context.transportMetadata()) {
    for (const existing of Object.keys(properties))
      if (existing.toLowerCase() === key.toLowerCase())
        Reflect.deleteProperty(properties, existing);
    properties[key] = value;
  }
  copy.application_properties = properties;
  const encoded = serialize(copy);
  if (encoded.length > MESSAGE_LIMIT || encoded.length > BYTE_LIMIT)
    throw new Error("RabbitMQ message exceeds 1 MiB admission profile");
  return encoded;
}
