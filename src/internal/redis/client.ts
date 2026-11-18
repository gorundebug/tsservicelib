import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import {
  createClient,
  RESP_TYPES,
  ErrorReply,
  ClientClosedError,
  ClientOfflineError,
  type RedisClientOptions
} from "redis";
import {
  Context,
  type MessageContext,
  type RedisStreamsDataConnectorConfig,
  type RedisStreamsEndpointConfig
} from "../../runtime/index.js";
import { wait, cancellation } from "./wait.js";
export { wait, cancellation } from "./wait.js";
export const PENDING_MESSAGES = 256;
export const BYTE_LIMIT = 8 * 1024 * 1024;
export const MESSAGE_LIMIT = 1024 * 1024;
export type FieldValue = readonly [string | Uint8Array, string | Uint8Array];
export type Fields = readonly FieldValue[];
export interface Entry {
  readonly id: string;
  readonly fields: readonly (readonly [Buffer, Buffer])[];
  readonly reclaimed: boolean;
  readonly bytes: number;
}
export interface Delivery {
  readonly outcome: "published" | "rejected" | "unknown" | "not-sent";
  readonly id?: string | undefined;
  readonly error?: Error | undefined;
}
export class DeletedPendingEntriesError extends Error {
  public constructor(public readonly ids: readonly string[]) {
    super(`RedisStreams XAUTOCLAIM removed ${String(ids.length)} deleted entries from the PEL`);
  }
}
function nativeClient(options: RedisClientOptions) {
  return createClient({
    ...options,
    RESP: 2,
    commandOptions: { typeMapping: { [RESP_TYPES.BLOB_STRING]: Buffer } }
  });
}
type Client = ReturnType<typeof nativeClient>;
function destroy(client: Client | undefined): void {
  if (client?.isOpen) client.destroy();
}
function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("RedisStreams unexpected native array reply");
  return value as unknown[];
}
function text(value: unknown): string {
  if (Buffer.isBuffer(value)) return value.toString();
  if (typeof value === "string") return value;
  throw new Error("RedisStreams unexpected native string reply");
}
export function entryBytes(fields: Fields): number {
  return fields.reduce((n, [k, v]) => n + Buffer.byteLength(k) + Buffer.byteLength(v) + 32, 0);
}
function entries(value: unknown, reclaimed: boolean): Entry[] {
  return array(value).map((raw) => {
    const [nativeId, values] = array(raw),
      id = text(nativeId),
      flat = array(values);
    if (flat.length % 2) throw new Error("RedisStreams malformed field/value reply");
    const fields: [Buffer, Buffer][] = [];
    let bytes = 0;
    for (let i = 0; i < flat.length; i += 2) {
      const k = flat[i],
        v = flat[i + 1];
      if (!Buffer.isBuffer(k) || !Buffer.isBuffer(v))
        throw new Error("RedisStreams requires binary native fields");
      bytes += k.length + v.length + 32;
      fields.push([k, v]);
    }
    // Oversized records carry only diagnostic identity, never a synthetic payload
    // into the graph. Copy admitted fields so tiny slices do not retain a large reply.
    return {
      id,
      fields:
        bytes > MESSAGE_LIMIT
          ? []
          : fields.map(([k, v]) => [Buffer.from(k), Buffer.from(v)] as const),
      reclaimed,
      bytes
    };
  });
}
export function snapshot(
  fields: Fields,
  context: MessageContext
): { fields: Fields; bytes: number } {
  if (!fields.length || fields.length > 4096)
    throw new Error("RedisStreams publication requires 1..4096 field/value pairs");
  const copy: [Buffer, Buffer][] = fields.map(([k, v]) => [Buffer.from(k), Buffer.from(v)]);
  for (const [key, value] of context.transportMetadata()) {
    for (let i = copy.length - 1; i >= 0; i--)
      if (copy[i]?.[0].toString().toLowerCase() === key.toLowerCase()) copy.splice(i, 1);
    copy.push([Buffer.from(key), Buffer.from(value)]);
  }
  const bytes = entryBytes(copy);
  if (bytes > MESSAGE_LIMIT) throw new Error("RedisStreams entry exceeds 1 MiB admission limit");
  return { fields: copy, bytes };
}
export function entryMetadata(entry: Entry): ReadonlyMap<string, string> {
  const result = new Map<string, string>();
  for (const [key, value] of entry.fields) {
    const k = key.toString().toLowerCase();
    if (["x-stream-id", "x-trace", "traceparent", "tracestate", "baggage"].includes(k))
      result.set(k, value.toString());
  }
  return result;
}
export class Session {
  public ready = false;
  public epoch = 0;
  public stopped = false;
  public paused = false;
  public readonly limit: number;
  public readonly consumerName: string;
  #control: Client | undefined;
  #reader: Client | undefined;
  #generationError: (error: Error) => void;
  #controller = new AbortController();
  #recovering: Promise<void> | undefined;
  #attempts = 0;
  #removeOwner: (() => void) | undefined;
  public constructor(
    public readonly config: RedisStreamsDataConnectorConfig,
    public readonly ep: RedisStreamsEndpointConfig,
    public readonly source: boolean,
    report: (error: Error) => void
  ) {
    this.limit = source ? (ep.readCount ?? 64) : (ep.publisherMaxInFlight ?? 256);
    this.consumerName = ep.consumerName ?? `ts-${String(ep.id)}-${randomUUID()}`;
    this.#generationError = report;
  }
  public isReady(): boolean {
    return this.ready;
  }
  private isStopped(): boolean {
    return this.stopped;
  }
  public async start(context: Context): Promise<void> {
    const abort = (): void => {
      this.close();
    };
    context.signal().addEventListener("abort", abort, { once: true });
    this.#removeOwner = () => {
      context.signal().removeEventListener("abort", abort);
    };
    try {
      if (context.cancelled()) throw cancellation(context);
      await this.open(context);
    } catch (error) {
      this.close();
      throw error;
    }
  }
  private async open(context: Context): Promise<void> {
    if (this.isStopped()) throw new Error("RedisStreams stopped");
    const c = this.config,
      u = new URL(c.servers[0] ?? "");
    const secure = c.tlsEnabled === true || u.protocol === "rediss:";
    const [ca, cert, key] = await wait(
      Promise.all([
        c.tlsCaFile ? readFile(c.tlsCaFile) : undefined,
        c.tlsCertFile ? readFile(c.tlsCertFile) : undefined,
        c.tlsKeyFile ? readFile(c.tlsKeyFile) : undefined
      ]),
      context,
      c.connectTimeoutMs
    );
    if (context.cancelled() || this.isStopped()) throw new Error("RedisStreams startup cancelled");
    const options: RedisClientOptions = {
      socket: {
        host: u.hostname.replace(/^\[|\]$/gu, ""),
        port: Number(u.port || "6379"),
        connectTimeout: c.connectTimeoutMs,
        reconnectStrategy: false,
        ...(secure
          ? {
              tls: true as const,
              rejectUnauthorized: true,
              servername: c.tlsServerName === "" ? u.hostname : (c.tlsServerName ?? u.hostname),
              ...(ca ? { ca } : {}),
              ...(cert ? { cert, key } : {})
            }
          : { tls: false as const })
      },
      database: c.redisDatabase,
      disableOfflineQueue: true,
      commandsQueueMaxLength: this.limit + 4,
      ...(c.username ? { username: c.username } : {}),
      ...(c.password ? { password: c.password } : {})
    };
    const control = nativeClient(options),
      reader = this.source ? nativeClient(options) : undefined;
    this.#control = control;
    this.#reader = reader;
    const epoch = ++this.epoch;
    for (const client of [control, reader])
      client?.on("error", () => {
        if (this.ready && epoch === this.epoch && !(client === reader && this.paused))
          this.invalidate(new Error("RedisStreams native connection failed"));
      });
    try {
      await wait(Promise.all([control.connect(), reader?.connect()]), context, c.connectTimeoutMs);
      const info = text(await this.direct(control, ["INFO", "server"], context));
      const version = /redis_version:(\d+)\.(\d+)/u.exec(info);
      if (
        !version ||
        Number(version[1]) < 7 ||
        (Number(version[1]) === 7 && Number(version[2]) < 2)
      )
        throw new Error("RedisStreams requires Redis 7.2 or newer");
      if (!/redis_mode:standalone\r?\n/u.test(info))
        throw new Error("RedisStreams only supports standalone; Sentinel/Cluster are unsupported");
      const role = array(await this.direct(control, ["ROLE"], context));
      if (text(role[0]) !== "master")
        throw new Error("RedisStreams requires a standalone primary, not a replica");
      if (this.source) {
        if (this.ep.createConsumerGroup) {
          try {
            await this.direct(
              control,
              [
                "XGROUP",
                "CREATE",
                this.ep.streamKey,
                this.ep.consumerGroup ?? "",
                this.ep.groupStartId ?? "0-0",
                "MKSTREAM"
              ],
              context
            );
          } catch (error) {
            if (!(error instanceof ErrorReply && error.message.startsWith("BUSYGROUP ")))
              throw error;
          }
        }
        const groups = array(
          await this.direct(control, ["XINFO", "GROUPS", this.ep.streamKey], context)
        );
        if (
          !groups.some((g) => {
            const parts = array(g);
            for (let i = 0; i < parts.length; i += 2)
              if (text(parts[i]) === "name" && text(parts[i + 1]) === this.ep.consumerGroup)
                return true;
            return false;
          })
        )
          throw new Error("RedisStreams consumer group is absent; creation is opt-in");
      } else {
        const kind = text(await this.direct(control, ["TYPE", this.ep.streamKey], context));
        if (!["none", "stream"].includes(kind))
          throw new Error("RedisStreams streamKey exists with an incompatible Redis type");
      }
      if (context.cancelled() || this.isStopped())
        throw new Error("RedisStreams startup cancelled");
      this.ready = true;
    } catch (error) {
      destroy(control);
      destroy(reader);
      this.#control = undefined;
      this.#reader = undefined;
      if (error instanceof Error && error.message.startsWith("RedisStreams")) throw error;
      // Native authentication errors are intentionally sanitized before leaving the transport.
      // eslint-disable-next-line preserve-caught-error
      throw new Error(
        "RedisStreams startup failed: verify authentication, permissions (INFO/ROLE/stream commands), database and resources"
      );
    }
  }
  private direct(
    client: Client,
    args: (string | Buffer)[],
    context: Context,
    timeout = this.config.operationTimeoutMs
  ): Promise<unknown> {
    return wait(client.sendCommand<unknown>(args), context, timeout);
  }
  public invalidate(error: Error): void {
    if (this.stopped || this.#recovering) return;
    this.ready = false;
    this.epoch++;
    destroy(this.#control);
    destroy(this.#reader);
    this.#control = undefined;
    this.#reader = undefined;
    this.#generationError(error);
    this.#recovering = (async () => {
      while (!this.isStopped() && !this.paused && this.#attempts++ < 60) {
        try {
          const context = new Context().withExternalCancellation(this.#controller.signal);
          await wait(new Promise<void>((resolve) => setTimeout(resolve, 250)), context);
          await this.open(context);
          return;
        } catch {
          if (!this.isStopped()) this.#generationError(new Error("RedisStreams reconnect failed"));
        }
      }
      if (!this.isStopped() && !this.paused) {
        this.#generationError(new Error("RedisStreams reconnect attempts exhausted"));
        this.close();
      }
    })().finally(() => {
      this.#recovering = undefined;
    });
    void this.#recovering.catch(() => undefined);
  }
  private async command(
    args: (string | Buffer)[],
    context: Context,
    read = false,
    timeout = this.config.operationTimeoutMs
  ): Promise<unknown> {
    const client = read ? this.#reader : this.#control;
    if (!this.ready || !client?.isReady || this.isStopped()) throw new ClientOfflineError();
    if (context.cancelled()) throw cancellation(context);
    try {
      return await this.direct(client, args, context, timeout);
    } catch (error) {
      if (!(error instanceof ErrorReply) && !(read && this.paused))
        this.invalidate(new Error("RedisStreams command interrupted; outcome may be unknown"));
      throw error;
    }
  }
  public async publish(fields: Fields, context: Context): Promise<Delivery> {
    if (context.cancelled() || !this.ready)
      return {
        outcome: "not-sent",
        error: new Error("RedisStreams cancelled or unavailable before XADD")
      };
    try {
      const reply = await this.command(
        [
          "XADD",
          this.ep.streamKey,
          ...(this.ep.createStream ? [] : ["NOMKSTREAM"]),
          "*",
          ...fields.flatMap(([k, v]) => [Buffer.from(k), Buffer.from(v)])
        ],
        context
      );
      if (reply === null)
        return {
          outcome: "rejected",
          error: new Error("RedisStreams XADD NOMKSTREAM: stream is absent")
        };
      return { outcome: "published", id: text(reply) };
    } catch (error) {
      return {
        outcome:
          error instanceof ErrorReply
            ? "rejected"
            : error instanceof ClientClosedError || error instanceof ClientOfflineError
              ? "not-sent"
              : "unknown",
        error:
          error instanceof ErrorReply
            ? error
            : new Error("RedisStreams publication interrupted; outcome may be unknown")
      };
    }
  }
  public async ack(id: string, epoch: number, context: Context): Promise<number> {
    if (epoch !== this.epoch || !this.ready)
      throw new Error("RedisStreams ACK handle belongs to an expired connection");
    const value = await this.command(
      ["XACK", this.ep.streamKey, this.ep.consumerGroup ?? "", id],
      context
    );
    if (typeof value !== "number") throw new Error("RedisStreams unexpected XACK reply");
    return value;
  }
  public async read(
    count: number,
    context: Context,
    block = this.ep.readBlockMs ?? 1000
  ): Promise<Entry[]> {
    const response = await this.command(
      [
        "XREADGROUP",
        "GROUP",
        this.ep.consumerGroup ?? "",
        this.consumerName,
        "COUNT",
        String(count),
        "BLOCK",
        String(block),
        "STREAMS",
        this.ep.streamKey,
        ">"
      ],
      context,
      true,
      block + this.config.operationTimeoutMs
    );
    if (response === null) return [];
    return array(response).flatMap((stream) => entries(array(stream)[1], false));
  }
  public async claim(
    cursor: string,
    count: number,
    context: Context
  ): Promise<{ cursor: string; entries: Entry[]; deleted: readonly string[] }> {
    const response = array(
      await this.command(
        [
          "XAUTOCLAIM",
          this.ep.streamKey,
          this.ep.consumerGroup ?? "",
          this.consumerName,
          String(this.ep.claimIdleMs ?? 60000),
          cursor,
          "COUNT",
          String(count)
        ],
        context
      )
    );
    return {
      cursor: text(response[0]),
      entries: entries(response[1], true),
      deleted: array(response[2]).map(text)
    };
  }
  public pause(): void {
    this.paused = true;
    destroy(this.#reader);
    this.#reader = undefined;
  }
  public close(): void {
    if (this.isStopped()) return;
    this.#removeOwner?.();
    this.#removeOwner = undefined;
    this.stopped = true;
    this.ready = false;
    this.epoch++;
    this.#controller.abort();
    destroy(this.#reader);
    destroy(this.#control);
    this.#reader = undefined;
    this.#control = undefined;
  }
}
