import assert from "node:assert/strict";
import { test } from "node:test";

import {
  DurableCaller,
  DurableCallContext,
  MessageContext,
  StringSerde,
  makeDurableLinkHandler,
  makeStreamSerde,
  type Completion,
  type Context,
  type DurableEnvelope,
  type DurableLinkHandler,
  type DurableLinkId,
  type DurableTransport,
  type RuntimeEnvironment,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";

class RecordingTransport implements DurableTransport {
  public readonly id = 7;
  public readonly name = "temporal";
  public readonly submitted: DurableEnvelope[] = [];
  public readonly submittedContexts: MessageContext[] = [];
  public readonly handlers = new Map<string, DurableLinkHandler>();

  public start(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public stopAdmission(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public stop(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public registerLink(link: DurableLinkId, handler: DurableLinkHandler): void {
    this.handlers.set(`${String(link.from)}:${String(link.to)}`, handler);
  }

  public submitLink(
    context: MessageContext,
    _link: DurableLinkId,
    envelope: DurableEnvelope
  ): Promise<void> {
    this.submittedContexts.push(context);
    this.submitted.push(envelope);
    return Promise.resolve();
  }
}

class RecordingConsumer implements TypedStreamConsumer<string> {
  public readonly id = 2;
  public readonly name = "target";
  public readonly transformationName = "map";
  public received: { readonly context: MessageContext; readonly value: string } | undefined;

  public consume(context: MessageContext, value: string): Completion {
    this.received = { context, value };
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    throw new Error("not used by this boundary test");
  }

  public config(): StreamConfig {
    throw new Error("not used by this boundary test");
  }
}

await test("DurableCall serializes transport context and leaves the target consumer unchanged", async () => {
  const transport = new RecordingTransport();
  const serde = makeStreamSerde(new StringSerde());
  const link = { from: 1, to: 2 };
  const caller = new DurableCaller(link, transport, serde);
  const context = new MessageContext()
    .withMetadata(
      new Map([["traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"]])
    )
    .withStreamId("request-1")
    .withPriority(-1)
    .withSampling(true)
    .bounded(30_000);

  await caller.consume(context, "payload");
  const envelope = transport.submitted[0];
  assert.ok(envelope);
  assert.equal(envelope.streamId, "request-1");
  assert.equal(envelope.priority, -1);
  assert.equal(transport.submittedContexts[0], context);
  assert.equal(serde.deserialize(envelope.payload), "payload");

  const consumer = new RecordingConsumer();
  const cancellation = new AbortController();
  await makeDurableLinkHandler(consumer, serde)(envelope, context, cancellation.signal);
  assert.ok(consumer.received);
  assert.equal(consumer.received.value, "payload");
  assert.equal(consumer.received.context.streamId(), "request-1");
  assert.equal(consumer.received.context.priority(), -1);
  assert.equal(consumer.received.context.samplingEnabled(), true);
  assert.equal(
    consumer.received.context.metadata().get("traceparent"),
    "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
  );
  cancellation.abort();
  assert.equal(consumer.received.context.signal().aborted, true);
});

await test("nested DurableCall identities are stable across Activity retry and distinct per emission", async () => {
  const transport = new RecordingTransport();
  const serde = makeStreamSerde(new StringSerde());
  const caller = new DurableCaller({ from: 2, to: 3 }, transport, serde);
  const executeAttempt = async (): Promise<readonly string[]> => {
    const context = new MessageContext().withDurableCallContext(
      new DurableCallContext("parent-call")
    );
    await caller.consume(context, "same");
    await caller.consume(context, "same");
    return transport.submitted.splice(0).map(({ callId }) => callId);
  };

  const first = await executeAttempt();
  const retry = await executeAttempt();
  assert.equal(first.length, 2);
  assert.notEqual(first[0], first[1]);
  assert.deepEqual(retry, first);
});
