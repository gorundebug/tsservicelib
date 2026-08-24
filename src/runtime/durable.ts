import { createHash, randomUUID } from "node:crypto";

import { MessageContext, type Context } from "./context.js";
import type { Lifecycle } from "./lifecycle.js";
import type { StreamSerde } from "./serde/index.js";
import type { Caller, TypedStreamConsumer } from "./stream.js";

export interface DurableLinkId {
  readonly from: number;
  readonly to: number;
}

/** Portable transport envelope. Target business functions remain unchanged. */
export interface DurableEnvelope {
  readonly version: number;
  readonly from: number;
  readonly to: number;
  readonly callId: string;
  readonly streamId: string;
  readonly priority: number;
  readonly deadlineUnixMillis: number;
  readonly samplingEnabled: boolean;
  readonly traceCarrier: Readonly<Record<string, string>>;
  readonly payload: Uint8Array;
}

export type DurableLinkHandler = (
  envelope: DurableEnvelope,
  cancellationSignal?: AbortSignal
) => Promise<void>;

/** Infrastructure transport used by DurableCall; it never replaces the target node. */
export interface DurableTransport extends Lifecycle {
  readonly id: number;
  readonly name: string;
  stopAdmission(context: Context): Promise<void>;
  registerLink(link: DurableLinkId, handler: DurableLinkHandler): void;
  submitLink(link: DurableLinkId, envelope: DurableEnvelope): Promise<void>;
}

export class DurableCaller<T> implements Caller<T> {
  public constructor(
    private readonly link: DurableLinkId,
    private readonly transport: DurableTransport,
    private readonly serde: StreamSerde<T>
  ) {}

  public isAsync(): boolean {
    return true;
  }

  public consume(context: MessageContext, value: T): Promise<void> {
    const payload = this.serde.serialize(value);
    const streamId = context.streamId() ?? randomUUID();
    const remainingMs = context.remainingMs();
    return this.transport.submitLink(this.link, {
      version: 1,
      from: this.link.from,
      to: this.link.to,
      callId: nextDurableCallId(context, this.link, payload),
      streamId,
      priority: context.priority() ?? 0,
      deadlineUnixMillis:
        remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
      samplingEnabled: context.samplingEnabled(),
      traceCarrier: Object.fromEntries(context.transportMetadata()),
      payload
    });
  }
}

export function makeDurableLinkHandler<T>(
  consumer: TypedStreamConsumer<T>,
  serde: StreamSerde<T>
): DurableLinkHandler {
  return async (envelope, cancellationSignal): Promise<void> => {
    if (envelope.version !== 1 || envelope.from <= 0 || envelope.to <= 0 || !envelope.callId) {
      throw new Error("invalid DurableCall envelope");
    }
    let context = new MessageContext()
      .withMetadata(new Map(Object.entries(envelope.traceCarrier)))
      .withStreamId(envelope.streamId || randomUUID())
      .withPriority(envelope.priority)
      .withSampling(envelope.samplingEnabled)
      .withDurableInvocation(envelope.callId);
    if (cancellationSignal !== undefined) {
      context = context.withExternalCancellation(cancellationSignal);
    }
    if (envelope.deadlineUnixMillis > 0) {
      context = context.bounded(Math.max(0, envelope.deadlineUnixMillis - Date.now()));
    }
    await consumer.consume(context, serde.deserialize(envelope.payload));
  };
}

function nextDurableCallId(
  context: MessageContext,
  link: DurableLinkId,
  payload: Uint8Array
): string {
  const invocation = context.durableInvocation();
  if (invocation === undefined) return randomUUID();
  const payloadDigest = createHash("sha256").update(payload).digest("hex");
  const key = `${String(link.from)}\0${String(link.to)}\0${payloadDigest}`;
  const occurrence = invocation.occurrence(key);
  return createHash("sha256")
    .update(`${invocation.parentCallId}\0${key}\0${String(occurrence)}`)
    .digest("hex");
}
