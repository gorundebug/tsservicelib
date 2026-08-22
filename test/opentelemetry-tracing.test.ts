import assert from "node:assert/strict";
import { test } from "node:test";

import { ExportResultCode, type ExportResult } from "@opentelemetry/core";
import type { ReadableSpan, SpanExporter } from "@opentelemetry/sdk-trace-node";
import {
  Context,
  MessageContext,
  SpanStatusCode,
  int64Attribute,
  stringAttribute
} from "@gorundebug/tsservicelib/runtime";
import { opentelemetry } from "@gorundebug/tsservicelib/runtime/telemetry";

class RecordingSpanExporter implements SpanExporter {
  readonly #spans: ReadableSpan[] = [];

  public export(spans: ReadableSpan[], resultCallback: (result: ExportResult) => void): void {
    this.#spans.push(...spans);
    resultCallback({ code: ExportResultCode.SUCCESS });
  }

  public forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  public shutdown(): Promise<void> {
    return Promise.resolve();
  }

  public spans(): readonly ReadableSpan[] {
    return [...this.#spans];
  }
}

await test("OpenTelemetry tracing exports an explicitly sampled root with W3C metadata", async () => {
  const exporter = new RecordingSpanExporter();
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    exporter,
    batch: { scheduledDelayMillis: 1 }
  });
  const context = new MessageContext().withMetadata(
    new Map([
      ["x-trace", "1"],
      ["baggage", "tenant=alpha"]
    ])
  );
  const started = engine
    .tracing()
    .tracer("orderservice")
    .start(context, "http.input", [stringAttribute("stream", "processOrder")]);

  assert.notEqual(started.context.openTelemetryContext(), undefined);
  assert.match(
    started.context.metadata().get("traceparent") ?? "",
    /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/
  );
  assert.equal(started.context.metadata().get("baggage"), "tenant=alpha");
  assert.equal(started.span.spanContext().isValid, true);

  started.span.setAttributes([int64Attribute("status_code", 200n)]);
  started.span.addEvent("http_call", [stringAttribute("route", "/v1/processorder")]);
  started.span.setStatus(SpanStatusCode.Ok, "ignored for OK");
  started.span.end();
  started.span.end();
  await engine.shutdown(Context.background());

  const spans = exporter.spans();
  assert.equal(spans.length, 1);
  const span = spans[0];
  assert.ok(span);
  assert.equal(span.name, "http.input");
  assert.equal(span.instrumentationScope.name, "orderservice");
  assert.equal(span.resource.attributes["service.name"], "orderservice");
  assert.equal(span.attributes["stream"], "processOrder");
  assert.equal(span.attributes["status_code"], 200);
  assert.deepEqual(
    span.events.map(({ name }) => name),
    ["http_call"]
  );
});

await test("OpenTelemetry tracing continues a sampled remote parent explicitly", async () => {
  const exporter = new RecordingSpanExporter();
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "inventoryservice",
    exporter
  });
  const parentTraceId = "0123456789abcdef0123456789abcdef";
  const parentSpanId = "0123456789abcdef";
  const context = new MessageContext().withMetadata(
    new Map([["traceparent", `00-${parentTraceId}-${parentSpanId}-01`]])
  );
  const started = engine.tracing().tracer("inventoryservice").start(context, "grpc.input");
  started.span.end();
  await engine.shutdown(Context.background());

  const spans = exporter.spans();
  assert.equal(spans.length, 1);
  const span = spans[0];
  assert.ok(span);
  assert.equal(span.spanContext().traceId, parentTraceId);
  assert.equal(span.parentSpanContext?.spanId, parentSpanId);
  assert.notEqual(span.spanContext().spanId, parentSpanId);
});

await test("OpenTelemetry child spans use only the explicitly returned MessageContext", async () => {
  const exporter = new RecordingSpanExporter();
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    exporter
  });
  const tracer = engine.tracing().tracer("orderservice");
  const firstRoot = tracer.start(
    new MessageContext().withMetadata(new Map([["x-trace", "1"]])),
    "http.input"
  );
  const secondRoot = tracer.start(
    new MessageContext().withMetadata(new Map([["x-trace", "1"]])),
    "http.input"
  );
  const child = tracer.start(firstRoot.context, "http.output");

  child.span.end();
  secondRoot.span.end();
  firstRoot.span.end();
  await engine.shutdown(Context.background());

  const spans = exporter.spans();
  const first = spans.find(
    (span) =>
      span.name === "http.input" &&
      span.spanContext().spanId === firstRoot.span.spanContext().spanId
  );
  const second = spans.find(
    (span) =>
      span.name === "http.input" &&
      span.spanContext().spanId === secondRoot.span.spanContext().spanId
  );
  const output = spans.find(({ name }) => name === "http.output");
  assert.ok(first);
  assert.ok(second);
  assert.ok(output);
  assert.notEqual(first.spanContext().traceId, second.spanContext().traceId);
  assert.equal(output.spanContext().traceId, first.spanContext().traceId);
  assert.equal(output.parentSpanContext?.spanId, first.spanContext().spanId);
});

await test("OpenTelemetry parent sampling cannot be overridden by an unsampled remote parent", async () => {
  const exporter = new RecordingSpanExporter();
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    exporter
  });
  const context = new MessageContext().withMetadata(
    new Map([
      ["x-trace", "1"],
      ["traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"]
    ])
  );
  const started = engine.tracing().tracer("orderservice").start(context, "http.input");
  started.span.end();
  await engine.shutdown(Context.background());

  assert.equal(exporter.spans().length, 0);
  assert.match(started.context.metadata().get("traceparent") ?? "", /-00$/);
});

await test("OpenTelemetry rejects int64 attributes that JavaScript cannot represent exactly", async () => {
  const exporter = new RecordingSpanExporter();
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    exporter
  });
  assert.throws(
    () =>
      engine
        .tracing()
        .tracer("orderservice")
        .start(new MessageContext(), "invalid", [
          int64Attribute("sequence", BigInt(Number.MAX_SAFE_INTEGER) + 1n)
        ]),
    /cannot represent int64 attribute sequence=/
  );
  await engine.shutdown(Context.background());
});

await test("OpenTelemetry tracing shutdown obeys cancellation and can finish later", async () => {
  let resolveShutdown: (() => void) | undefined;
  class BlockingSpanExporter extends RecordingSpanExporter {
    public override shutdown(): Promise<void> {
      return new Promise<void>((resolve) => {
        resolveShutdown = resolve;
      });
    }
  }
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    exporter: new BlockingSpanExporter()
  });
  const controller = new AbortController();
  const shutdown = engine.shutdown(new Context(controller.signal));
  controller.abort(new Error("deadline exceeded"));
  await assert.rejects(shutdown, /deadline exceeded/);
  resolveShutdown?.();
  await engine.shutdown(Context.background());
});

await test("OpenTelemetry tracing does not fail application spans when the collector is unavailable", async () => {
  const engine = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    endpoint: "http://127.0.0.1:1",
    exportTimeoutMillis: 50,
    batch: {
      scheduledDelayMillis: 1,
      exportTimeoutMillis: 50,
      maxExportBatchSize: 1
    }
  });
  const started = engine
    .tracing()
    .tracer("orderservice")
    .start(new MessageContext(), "collector unavailable");
  assert.doesNotThrow(() => {
    started.span.end();
  });
  await assert.doesNotReject(engine.shutdown(Context.background()));
});
