import { createHash, randomUUID } from "node:crypto";
import { bindDurableCallSpan } from "./durable-call-context.js";
import { stringAttribute } from "./environment/index.js";
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
        return this.transport.submitLink(context, this.link, {
            version: 1,
            from: this.link.from,
            to: this.link.to,
            callId: nextDurableCallId(context, this.link, payload),
            streamId,
            priority: context.priority() ?? 0,
            deadlineUnixMillis: remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
            payload
        });
    }
}
export function makeDurableLinkHandler(consumer, serde) {
    return async (envelope, parent, cancellationSignal, durableCallContext) => {
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
        let span;
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
        }
        finally {
            if (!durableSpan)
                span?.end();
        }
    };
}
function nextDurableCallId(context, link, payload) {
    const invocation = context.durableCallContext();
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