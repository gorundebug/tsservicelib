import { readFile } from "node:fs/promises";
import {
  connect,
  credsAuthenticator,
  type MsgHdrs,
  type NatsConnection,
  type NodeConnectionOptions
} from "@nats-io/transport-node";
import {
  jetstream,
  jetstreamManager,
  AckPolicy,
  DeliverPolicy,
  ReplayPolicy,
  StorageType,
  JetStreamApiError,
  type Consumer,
  type JetStreamClient
} from "@nats-io/jetstream";
import {
  errorFromUnknown,
  type Context,
  type NATSDataConnectorConfig,
  type NATSEndpointConfig
} from "../../runtime/index.js";

export const PENDING_MESSAGES = 256;
export const PENDING_BYTES = 8 * 1024 * 1024;

export function cancellation(context: Context): Error {
  return context.signal().reason === undefined
    ? new Error("NATS operation cancelled")
    : errorFromUnknown(context.signal().reason);
}

/** Observes late rejection, removes listeners, and never interprets cancellation as withdrawal. */
export function wait<T>(operation: Promise<T>, context: Context, timeoutMs?: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const finish = (error: Error | undefined, value?: T): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      context.signal().removeEventListener("abort", abort);
      if (error !== undefined) reject(error);
      else resolve(value as T);
    };
    const abort = (): void => {
      finish(cancellation(context));
    };
    const remaining = Math.min(timeoutMs ?? Infinity, context.remainingMs() ?? Infinity);
    const deadline = performance.now() + remaining;
    if (Number.isFinite(remaining)) {
      const tick = (): void => {
        const left = deadline - performance.now();
        if (left <= 0)
          finish(new Error("NATS operation deadline exceeded; delivery may be unknown"));
        else timer = setTimeout(tick, Math.min(2147483647, Math.ceil(left)));
      };
      timer = setTimeout(tick, Math.min(2147483647, Math.max(0, Math.ceil(remaining))));
    }
    context.signal().addEventListener("abort", abort, { once: true });
    operation.then(
      (value) => {
        if (context.cancelled()) finish(cancellation(context));
        else if (performance.now() >= deadline)
          finish(new Error("NATS operation deadline exceeded; delivery may be unknown"));
        else finish(undefined, value);
      },
      (error: unknown) => {
        finish(errorFromUnknown(error));
      }
    );
    if (context.cancelled()) abort();
  });
}

export async function openConnection(
  config: NATSDataConnectorConfig,
  owner: Context
): Promise<NatsConnection> {
  if (owner.cancelled()) throw cancellation(owner);
  // Node's native timer-backed SDK cannot represent a larger single timeout.
  if (config.connectTimeoutMs > 2147483647 || config.operationTimeoutMs > 2147483647)
    throw new Error(
      "NATS.js Node SDK supports timeouts up to 2147483647 ms; canonical schema permits larger values unsupported by this runtime"
    );
  const options: NodeConnectionOptions = {
    servers: [...config.servers],
    timeout: config.connectTimeoutMs,
    reconnect: true,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 250,
    reconnectJitter: 100,
    reconnectJitterTLS: 100
  };
  if (config.username && config.password) {
    options.user = config.username;
    options.pass = config.password;
  }
  if (config.token) options.token = config.token;
  if (config.credentialsFile)
    options.authenticator = credsAuthenticator(
      await wait(readFile(config.credentialsFile), owner, config.connectTimeoutMs)
    );
  if (
    config.tlsEnabled ||
    config.tlsServerName ||
    config.tlsCaFile ||
    config.tlsCertFile ||
    config.servers.some((s) => new URL(s).protocol === "tls:")
  ) {
    options.tls = { rejectUnauthorized: true };
    if (config.tlsCaFile) options.tls.caFile = config.tlsCaFile;
    if (config.tlsCertFile) options.tls.certFile = config.tlsCertFile;
    if (config.tlsKeyFile) options.tls.keyFile = config.tlsKeyFile;
  }
  const pending = connect(options);
  let nc: NatsConnection;
  try {
    nc = await wait(pending, owner, config.connectTimeoutMs);
  } catch (error) {
    void pending.then(
      (connection) => connection.close(),
      () => undefined
    );
    throw error;
  }
  const close = (): void => {
    void nc.close();
  };
  owner.signal().addEventListener("abort", close, { once: true });
  void nc.closed().then(() => {
    owner.signal().removeEventListener("abort", close);
  });
  if (owner.cancelled()) {
    await nc.close();
    throw cancellation(owner);
  }
  return nc;
}

/** Checks containment of the whole endpoint subject language, not just one concrete sample. */
export function subjectCovered(filter: string, subject: string): boolean {
  const a = filter.split(".");
  const b = subject.split(".");
  for (let i = 0; i < a.length; i++) {
    if (a[i] === ">") return i < b.length;
    if (b[i] === undefined || b[i] === ">" || (a[i] !== "*" && a[i] !== b[i])) return false;
  }
  return a.length === b.length;
}

export async function resources(
  nc: NatsConnection,
  config: NATSEndpointConfig,
  timeout: number,
  source: boolean
): Promise<{ js: JetStreamClient; consumer?: Consumer }> {
  const streamName = config.streamName;
  if (!streamName) throw new Error("JetStream streamName required");
  const manager = await jetstreamManager(nc, { timeout });
  let stream;
  try {
    stream = await manager.streams.info(streamName);
  } catch (error) {
    if (!(error instanceof JetStreamApiError && error.code === 10059) || !config.createStream)
      throw error;
    stream = await manager.streams.add({
      name: streamName,
      subjects: [config.subject],
      storage: StorageType.File,
      num_replicas: 1
    });
  }
  if (!stream.config.subjects.some((subject) => subjectCovered(subject, config.subject)))
    throw new Error("existing JetStream stream does not cover endpoint subject");
  const js = jetstream(nc, { timeout });
  if (!source) return { js };
  const durableName = config.durableName;
  if (!durableName) throw new Error("JetStream durableName required");
  let info;
  try {
    info = await manager.consumers.info(streamName, durableName);
  } catch (error) {
    if (!(error instanceof JetStreamApiError && error.code === 10014) || !config.createConsumer)
      throw error;
    // add uses the native create-only action, unlike addOrUpdate.
    info = await manager.consumers.add(streamName, {
      durable_name: durableName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
      filter_subject: config.subject,
      max_ack_pending: PENDING_MESSAGES
    });
  }
  if (
    info.config.ack_policy !== AckPolicy.Explicit ||
    info.config.deliver_subject ||
    info.config.durable_name !== config.durableName ||
    info.config.filter_subject !== config.subject ||
    info.config.filter_subjects?.length
  )
    throw new Error(
      "existing JetStream consumer must be durable pull, explicit ACK, and match the endpoint filter"
    );
  return { js, consumer: await js.consumers.get(streamName, config.durableName) };
}

export function messageBytes(data: Uint8Array, subject: string, headers?: MsgHdrs): number {
  let bytes = data.byteLength + Buffer.byteLength(subject) + 32;
  if (headers)
    for (const [name, values] of headers)
      for (const value of values) bytes += Buffer.byteLength(name) + Buffer.byteLength(value) + 4;
  return bytes;
}
