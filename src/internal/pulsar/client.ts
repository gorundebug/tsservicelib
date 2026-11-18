import { readFile } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import Pulsar from "pulsar-client";
import {
  type Context,
  errorFromUnknown,
  requirePulsarDataConnectorConfig,
  validatePulsarEndpoint,
  type MessageContext,
  type PulsarDataConnectorConfig,
  type PulsarEndpointConfig
} from "../../runtime/index.js";
import { cancellation, wait } from "./wait.js";
export { cancellation, wait } from "./wait.js";
export type { ProducerMessage, Message, MessageId } from "pulsar-client";
export const BYTE_LIMIT = 8 * 1024 * 1024;
export const MESSAGE_LIMIT = 1024 * 1024;
export const PENDING_MESSAGES = 4096;
export interface Delivery {
  readonly outcome: "NotSent" | "Accepted" | "Rejected" | "Unknown";
  readonly id?: Pulsar.MessageId | undefined;
  readonly error?: Error | undefined;
}
export class AdmissionRejected extends Error {
  public readonly outcome = "NotSent";
}
export function snapshot(
  message: Pulsar.ProducerMessage | undefined,
  context: MessageContext
): { message: Pulsar.ProducerMessage; bytes: number } {
  if (!message || !Buffer.isBuffer(message.data))
    throw new AdmissionRejected("Pulsar requires a binary Buffer payload");
  for (const key of [
    "sequenceId",
    "orderingKey",
    "replicationClusters",
    "deliverAfter",
    "deliverAt",
    "disableReplication"
  ])
    if (key in message) throw new AdmissionRejected(`Pulsar unsupported message option ${key}`);
  if (message.partitionKey !== undefined && typeof message.partitionKey !== "string")
    throw new AdmissionRejected("Pulsar partitionKey must be a string");
  if (
    message.eventTimestamp !== undefined &&
    (!Number.isSafeInteger(message.eventTimestamp) || message.eventTimestamp < 0)
  )
    throw new AdmissionRejected(
      "Pulsar eventTimestamp requires nonnegative safe integer milliseconds"
    );
  const properties = { ...message.properties };
  if (
    Object.keys(properties).length > PENDING_MESSAGES ||
    Object.values(properties).some((value) => typeof value !== "string")
  )
    throw new AdmissionRejected("Pulsar properties require bounded string pairs");
  for (const [key, value] of context.transportMetadata()) {
    for (const existing of Object.keys(properties))
      if (existing.toLowerCase() === key.toLowerCase())
        Reflect.deleteProperty(properties, existing);
    properties[key] = value;
  }
  const bytes =
    message.data.length +
    Buffer.byteLength(message.partitionKey ?? "") +
    Object.entries(properties).reduce(
      (n, [k, v]) => n + Buffer.byteLength(k) + Buffer.byteLength(v) + 32,
      0
    );
  if (bytes > MESSAGE_LIMIT) throw new AdmissionRejected("Pulsar message exceeds 1 MiB");
  return { message: { ...message, data: Buffer.from(message.data), properties }, bytes };
}
export function messageMetadata(message: Pulsar.Message): ReadonlyMap<string, string> {
  return new Map(Object.entries(message.getProperties()).map(([k, v]) => [k.toLowerCase(), v]));
}
export function nativeFailure(reason: unknown): Delivery {
  const error = errorFromUnknown(reason);
  // v1.18.0 src/Producer.cc emits this exact prefix plus pulsar_result_str.
  // No substring/regex inference; unknown versions/results remain ambiguous.
  const notSent = [
    "ProducerQueueIsFull",
    "MemoryBufferIsFull",
    "MessageTooBig",
    "InvalidMessage",
    "ProducerNotInitialized"
  ];
  const rejected = [
    "AuthenticationError",
    "AuthorizationError",
    "TopicTerminated",
    "ProducerFenced",
    "IncompatibleSchema",
    "NotAllowedError"
  ];
  if (notSent.some((code) => error.message === `Failed to send message: ${code}`))
    return { outcome: "NotSent", error };
  if (rejected.some((code) => error.message === `Failed to send message: ${code}`))
    return { outcome: "Rejected", error };
  return { outcome: "Unknown", error }; // Includes AlreadyClosed after an admitted send.
}
export class AdminError extends Error {
  public constructor(public readonly status: number) {
    super(`Pulsar administration returned HTTP ${String(status)}`);
  }
}
export async function ensureResources(
  c: PulsarDataConnectorConfig,
  e: PulsarEndpointConfig,
  source: boolean,
  context: Context
): Promise<void> {
  requirePulsarDataConnectorConfig(c);
  validatePulsarEndpoint(e, !source);
  const adminToken = (c.adminToken?.length ?? 0) > 0 ? c.adminToken : c.token;
  const [ca, cert, key] = await wait(
    Promise.all([
      c.tlsCaFile ? readFile(c.tlsCaFile) : undefined,
      c.tlsCertFile ? readFile(c.tlsCertFile) : undefined,
      c.tlsKeyFile ? readFile(c.tlsKeyFile) : undefined
    ]),
    context,
    c.operationTimeoutMs
  );
  async function request(
    method: string,
    path: string,
    body?: unknown,
    rawBody?: string
  ): Promise<unknown> {
    if (context.cancelled()) throw cancellation(context);
    const url = new URL(c.adminUrl + path),
      data = rawBody ?? (body === undefined ? undefined : JSON.stringify(body));
    return new Promise((resolve, reject) => {
      let clearConnect: (() => void) | undefined;
      let finished = false,
        size = 0;
      const chunks: Buffer[] = [];
      const remaining = Math.min(c.operationTimeoutMs, context.remainingMs() ?? Infinity);
      const abort = (): void => {
        req.destroy(cancellation(context));
      };
      const finish = (error?: Error, value?: unknown): void => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        clearConnect?.();
        context.signal().removeEventListener("abort", abort);
        if (error) reject(error);
        else resolve(value);
      };
      const req = (url.protocol === "https:" ? httpsRequest : httpRequest)(
        url,
        {
          method,
          ca,
          cert,
          key,
          rejectUnauthorized: true,
          headers: {
            accept: "application/json",
            "content-type": "application/json",
            ...(adminToken ? { authorization: `Bearer ${adminToken}` } : {})
          }
        },
        (response) => {
          if (!response.statusCode || response.statusCode < 200 || response.statusCode >= 300) {
            response.resume();
            finish(new AdminError(response.statusCode ?? 0));
            return;
          }
          response.on("data", (chunk: Buffer) => {
            size += chunk.length;
            if (size > 4 * 1024 * 1024)
              req.destroy(new Error("Pulsar admin response exceeds limit"));
            else chunks.push(chunk);
          });
          response.on("error", (error: Error) => {
            finish(error);
          });
          response.on("end", () => {
            try {
              finish(
                undefined,
                size ? (JSON.parse(Buffer.concat(chunks).toString()) as unknown) : undefined
              );
            } catch {
              finish(new Error("Invalid Pulsar admin JSON"));
            }
          });
        }
      );
      const timer = setTimeout(
        () => req.destroy(new Error("Pulsar admin deadline exceeded")),
        remaining
      );
      req.once("error", (error: Error) => {
        finish(error);
      });
      req.once("socket", (socket) => {
        if (!socket.connecting || finished) return;
        const event = url.protocol === "https:" ? "secureConnect" : "connect";
        const connectTimer = setTimeout(
          () => req.destroy(new Error("Pulsar admin connection timeout")),
          c.connectTimeoutMs
        );
        clearConnect = (): void => {
          clearTimeout(connectTimer);
          if (clearConnect) socket.removeListener(event, clearConnect);
        };
        socket.once(event, clearConnect);
      });
      context.signal().addEventListener("abort", abort, { once: true });
      if (context.cancelled()) abort();
      req.end(data);
    });
  }
  const parts = e.topic.slice("persistent://".length).split("/").map(encodeURIComponent);
  const ns = `/admin/v2/namespaces/${parts.slice(0, 2).join("/")}`,
    topic = `/admin/v2/persistent/${parts.join("/")}`;
  for (const [policy, field] of [
    ["autoTopicCreation", "allowAutoTopicCreation"],
    ...(source ? [["autoSubscriptionCreation", "allowAutoSubscriptionCreation"]] : [])
  ]) {
    const value = await request("GET", `${ns}/${policy ?? ""}`);
    if (
      !value ||
      typeof value !== "object" ||
      (value as Record<string, unknown>)[field ?? ""] !== false
    )
      throw new Error(`Pulsar namespace must explicitly disable ${field ?? "creation"}`);
  }
  async function partitions(): Promise<number> {
    const value = await request("GET", topic + "/partitions?checkAllowAutoCreation=false");
    const count =
      value && typeof value === "object"
        ? (value as Record<string, unknown>)["partitions"]
        : undefined;
    if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0)
      throw new Error("Invalid Pulsar partition metadata");
    if (count === 0) await request("GET", topic + "/stats");
    return count;
  }
  let count: number;
  try {
    count = await partitions();
  } catch (error) {
    if (!(error instanceof AdminError) || error.status !== 404 || !e.createTopic) throw error;
    try {
      await request("PUT", topic + (e.partitions ? "/partitions" : ""), e.partitions);
    } catch (conflict) {
      if (!(conflict instanceof AdminError) || conflict.status !== 409) throw conflict;
    }
    count = await partitions();
  }
  if (e.createTopic && count !== (e.partitions ?? 0))
    throw new Error("Existing Pulsar partition count differs; no reconciliation");
  if (!source) return;
  async function hasSubscription(): Promise<boolean> {
    const list = await request("GET", topic + "/subscriptions");
    if (!Array.isArray(list)) throw new Error("Invalid Pulsar subscriptions reply");
    return list.includes(e.subscriptionName);
  }
  if (await hasSubscription()) return;
  if (!e.createSubscription) throw new Error("Pulsar subscription missing; creation disabled");
  // Preserve the exact int64 sentinel in JSON, without rounding through Number.
  const position = e.subscriptionInitialPosition === "Latest" ? 9223372036854775807n : -1n;
  try {
    await request(
      "PUT",
      topic + "/subscription/" + encodeURIComponent(e.subscriptionName ?? ""),
      undefined,
      `{"ledgerId":${position.toString()},"entryId":${position.toString()},"partitionIndex":-1}`
    );
  } catch (error) {
    if (!(error instanceof AdminError) || error.status !== 409) throw error;
  }
  if (!(await hasSubscription())) throw new Error("Pulsar subscription absent after creation");
}

export class Session {
  public ready = false;
  public stopped = false;
  public epoch = 0;
  public readonly limit: number;
  #client: Pulsar.Client | undefined;
  #producer: Pulsar.Producer | undefined;
  #consumer: Pulsar.Consumer | undefined;
  #opening: Promise<void> | undefined;
  #closing: Promise<void> | undefined;
  #receive: Promise<Pulsar.Message> | undefined;
  #receiving = false;
  #sends = new Set<Promise<Pulsar.MessageId>>();
  #sendBytes = 0;
  public constructor(
    public readonly config: PulsarDataConnectorConfig,
    public readonly ep: PulsarEndpointConfig,
    public readonly source: boolean,
    private readonly report: (error: Error) => void
  ) {
    requirePulsarDataConnectorConfig(config);
    validatePulsarEndpoint(ep, !source);
    this.limit = source ? (ep.receiverQueueSize ?? 256) : (ep.publisherMaxInFlight ?? 256);
  }
  public isReady(): boolean {
    return this.ready;
  }
  private isStopped(): boolean {
    return this.stopped;
  }
  public async start(context: Context): Promise<void> {
    if (this.#client || this.stopped) throw new Error("Pulsar session already started/stopped");
    await ensureResources(this.config, this.ep, this.source, context);
    if (context.cancelled() || this.isStopped()) throw cancellation(context);
    const c = this.config;
    const authentication = c.token
      ? new Pulsar.AuthenticationToken({ token: c.token })
      : c.tlsCertFile && c.tlsKeyFile
        ? new Pulsar.AuthenticationTls({
            certificatePath: c.tlsCertFile,
            privateKeyPath: c.tlsKeyFile
          })
        : undefined;
    this.#client = new Pulsar.Client({
      serviceUrl: c.servers[0] ?? "",
      operationTimeoutSeconds: Math.max(1, Math.ceil(c.operationTimeoutMs / 1000)),
      connectionTimeoutMs: c.connectTimeoutMs,
      tlsValidateHostname: true,
      tlsAllowInsecureConnection: false,
      ...(authentication ? { authentication } : {}),
      ...(c.tlsCaFile ? { tlsTrustCertsFilePath: c.tlsCaFile } : {}),
      ...(c.tlsCertFile && c.tlsKeyFile
        ? { tlsCertificateFilePath: c.tlsCertFile, tlsPrivateKeyFilePath: c.tlsKeyFile }
        : {})
    });
    const client = this.#client;
    this.#opening = (async () => {
      if (this.source)
        this.#consumer = await client.subscribe({
          topic: this.ep.topic,
          subscription: this.ep.subscriptionName ?? "",
          subscriptionType: this.ep.subscriptionType ?? "Shared",
          receiverQueueSize: this.limit,
          receiverQueueSizeAcrossPartitions: this.limit,
          nAckRedeliverTimeoutMs: this.ep.negativeAckRedeliveryDelayMs ?? 60000,
          batchIndexAckEnabled: true
        });
      else
        this.#producer = await client.createProducer({
          topic: this.ep.topic,
          batchingEnabled: false,
          blockIfQueueFull: false,
          maxPendingMessages: this.limit,
          maxPendingMessagesAcrossPartitions: this.limit,
          sendTimeoutMs: c.operationTimeoutMs
        });
      if (!this.stopped) {
        this.ready = true;
        this.epoch++;
      }
    })();
    try {
      await wait(this.#opening, context, c.operationTimeoutMs);
    } catch (error) {
      void this.close(context).catch(this.report);
      throw error;
    }
  }
  public async receive(context: Context): Promise<Pulsar.Message> {
    if (!this.ready || this.stopped || !this.#consumer || this.#receiving)
      throw new Error("Pulsar receive unavailable");
    this.#receiving = true;
    if (!this.#receive) {
      const operation = this.#consumer.receive(); // No timeout overload: native asynchronous C API.
      this.#receive = operation;
      void operation.catch(() => {
        if (this.#receive === operation) this.#receive = undefined;
      });
    }
    try {
      const message = await wait(this.#receive, context);
      this.#receive = undefined;
      return message;
    } finally {
      this.#receiving = false;
    }
  }
  public async settle(
    message: Pulsar.Message,
    epoch: number,
    context: Context,
    nack = false
  ): Promise<void> {
    if (context.cancelled()) throw cancellation(context);
    if (!this.ready || this.stopped || epoch !== this.epoch || !this.#consumer)
      throw new Error("Pulsar delivery scope ended");
    if (nack) {
      this.#consumer.negativeAcknowledge(message);
      return;
    }
    await wait(this.#consumer.acknowledge(message), context, this.config.operationTimeoutMs);
  }
  public async publish(
    message: Pulsar.ProducerMessage,
    context: MessageContext
  ): Promise<Delivery> {
    if (
      this.source ||
      !this.ready ||
      this.stopped ||
      !this.#producer ||
      this.#sends.size >= this.limit
    )
      return {
        outcome: "NotSent",
        error: new AdmissionRejected("Pulsar producer unavailable or full")
      };
    if (context.cancelled()) return { outcome: "NotSent", error: cancellation(context) };
    let copied: Pulsar.ProducerMessage;
    let bytes: number;
    try {
      const pending = snapshot(message, context);
      copied = pending.message;
      bytes = pending.bytes;
      if (this.#sendBytes + bytes > BYTE_LIMIT)
        throw new AdmissionRejected("Pulsar native pending byte admission limit exceeded");
    } catch (error) {
      return { outcome: "NotSent", error: errorFromUnknown(error) };
    }
    let operation: Promise<Pulsar.MessageId>;
    try {
      operation = this.#producer.send(copied);
    } catch (error) {
      return { outcome: "NotSent", error: errorFromUnknown(error) };
    }
    this.#sends.add(operation);
    this.#sendBytes += bytes;
    const release = (): void => {
      this.#sends.delete(operation);
      this.#sendBytes -= bytes;
    };
    void operation.then(release, release);
    try {
      return {
        outcome: "Accepted",
        id: await wait(operation, context, this.config.operationTimeoutMs)
      };
    } catch (error) {
      return nativeFailure(error);
    }
  }
  public close(context: Context): Promise<void> {
    this.stopped = true;
    this.ready = false;
    this.#closing ??= (async () => {
      this.epoch++;
      await this.#opening?.catch(() => undefined);
      if (this.#client) await this.#client.close();
      this.#receive = undefined;
    })();
    return wait(this.#closing, context, this.config.operationTimeoutMs);
  }
}
