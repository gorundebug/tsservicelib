import { createHash, randomUUID } from "node:crypto";

import type { Context, MessageContext } from "./context.js";
import { bindDurableCallSpan, type DurableCallContext } from "./durable-call-context.js";
import { stringAttribute, type Span } from "./environment/index.js";
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
  readonly payload: Uint8Array;
}

export type DurableLinkHandler = (
  envelope: DurableEnvelope,
  context: MessageContext,
  cancellationSignal?: AbortSignal,
  durableCallContext?: DurableCallContext
) => Promise<void>;

/** Infrastructure transport used by DurableCall; it never replaces the target node. */
export interface DurableTransport extends Lifecycle {
  readonly id: number;
  readonly name: string;
  stopAdmission(context: Context): Promise<void>;
  registerLink(link: DurableLinkId, handler: DurableLinkHandler): void;
  submitLink(
    context: MessageContext,
    link: DurableLinkId,
    envelope: DurableEnvelope
  ): Promise<void>;
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
    return this.transport.submitLink(context, this.link, {
      version: 1,
      from: this.link.from,
      to: this.link.to,
      callId: nextDurableCallId(context, this.link, payload),
      streamId,
      priority: context.priority() ?? 0,
      deadlineUnixMillis:
        remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
      payload
    });
  }
}

export function makeDurableLinkHandler<T>(
  consumer: TypedStreamConsumer<T>,
  serde: StreamSerde<T>
): DurableLinkHandler {
  return async (envelope, parent, cancellationSignal, durableCallContext): Promise<void> => {
    if (envelope.version !== 1 || envelope.from <= 0 || envelope.to <= 0 || !envelope.callId) {
      throw new Error("invalid DurableCall envelope");
    }
    let context = parent
      .withStreamId(envelope.streamId || randomUUID())
      .withPriority(envelope.priority);
    if (durableCallContext !== undefined) {
      context = context.withDurableCallContext(durableCallContext);
    }
    if (cancellationSignal !== undefined) {
      context = context.withExternalCancellation(cancellationSignal);
    }
    if (envelope.deadlineUnixMillis > 0) {
      context = context.bounded(Math.max(0, envelope.deadlineUnixMillis - Date.now()));
    }
    let span: Span | undefined;
    let durableSpan = false;
    if (durableCallContext !== undefined && context.samplingEnabled()) {
      const environment = consumer.runtimeEnvironment();
      const tracer = environment.tracing()?.tracer(environment.serviceConfig().name);
      if (tracer !== undefined) {
        const sourceName = environment.runtimeConfig().streamById(envelope.from)?.name;
        const started = tracer.start(context, "temporal.activity", [
          stringAttribute("boundary", "durable_call"),
          stringAttribute("from", sourceName ?? String(envelope.from)),
          stringAttribute("to", consumer.name)
        ]);
        context = started.context;
        span = started.span;
        durableSpan = bindDurableCallSpan(context, span);
      }
    }
    try {
      await consumer.consume(context, serde.deserialize(envelope.payload));
    } finally {
      if (!durableSpan) span?.end();
    }
  };
}

function nextDurableCallId(
  context: MessageContext,
  link: DurableLinkId,
  payload: Uint8Array
): string {
  const invocation = context.durableCallContext();
  if (invocation === undefined) return randomUUID();
  const payloadDigest = createHash("sha256").update(payload).digest("hex");
  const key = `${String(link.from)}\0${String(link.to)}\0${payloadDigest}`;
  const occurrence = invocation.occurrence(key);
  return createHash("sha256")
    .update(`${invocation.parentCallId}\0${key}\0${String(occurrence)}`)
    .digest("hex");
}
