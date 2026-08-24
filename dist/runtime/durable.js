import { createHash, randomUUID } from "node:crypto";
import { MessageContext } from "./context.js";
export class DurableCaller {
    link;
    transport;
    serde;
    constructor(link, transport, serde) {
        this.link = link;
        this.transport = transport;
        this.serde = serde;
    }
    isAsync() {
        return true;
    }
    consume(context, value) {
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
            deadlineUnixMillis: remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
            samplingEnabled: context.samplingEnabled(),
            traceCarrier: Object.fromEntries(context.transportMetadata()),
            payload
        });
    }
}
export function makeDurableLinkHandler(consumer, serde) {
    return async (envelope, cancellationSignal) => {
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
function nextDurableCallId(context, link, payload) {
    const invocation = context.durableInvocation();
    if (invocation === undefined)
        return randomUUID();
    const payloadDigest = createHash("sha256").update(payload).digest("hex");
    const key = `${String(link.from)}\0${String(link.to)}\0${payloadDigest}`;
    const occurrence = invocation.occurrence(key);
    return createHash("sha256")
        .update(`${invocation.parentCallId}\0${key}\0${String(occurrence)}`)
        .digest("hex");
}
//# sourceMappingURL=durable.js.map