import assert from "node:assert/strict";
import { test } from "node:test";

import { SeverityNumber } from "@opentelemetry/api-logs";
import { ExportResultCode, type ExportResult } from "@opentelemetry/core";
import type { LogRecordExporter, ReadableLogRecord } from "@opentelemetry/sdk-logs";
import type { ReadableSpan, SpanExporter } from "@opentelemetry/sdk-trace-node";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";

import {
  any,
  bool,
  Context,
  err,
  float64,
  int64,
  MessageContext,
  str
} from "@gorundebug/tsservicelib/runtime";
import { opentelemetry } from "@gorundebug/tsservicelib/runtime/telemetry";

class RecordingExporter implements LogRecordExporter {
  readonly records: ReadableLogRecord[] = [];
  shutdownCalled = false;

  public export(
    records: ReadableLogRecord[],
    resultCallback: (result: ExportResult) => void
  ): void {
    this.records.push(...records);
    resultCallback({ code: ExportResultCode.SUCCESS });
  }

  public forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  public shutdown(): Promise<void> {
    this.shutdownCalled = true;
    return Promise.resolve();
  }
}

class RecordingSpanExporter implements SpanExporter {
  public export(_spans: ReadableSpan[], callback: (result: ExportResult) => void): void {
    callback({ code: ExportResultCode.SUCCESS });
  }

  public forceFlush(): Promise<void> {
    return Promise.resolve();
  }

  public shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

await test("OpenTelemetry logs preserve the canonical structured contract and flush", async () => {
  const exporter = new RecordingExporter();
  const engine = new opentelemetry.OpenTelemetryLogsEngine({
    serviceName: "orderservice",
    exporter,
    batch: { scheduledDelayMillis: 60_000 },
    now: () => new Date("2026-08-18T00:00:00.000Z")
  });
  const logger = engine.defaultLogger();
  const context = Context.background();
  logger.debug(context, "debug");
  logger.info(context, "info");
  logger.warn(
    context,
    "request failed",
    str("service", "orderservice"),
    str("stream", "Process Order"),
    int64("safe", 42n),
    int64("exact_unsafe", 9_007_199_254_740_993n),
    float64("ratio", 1.5),
    bool("retry", true),
    err(new Error("timeout")),
    any("details", { attempt: 3 })
  );
  logger.error(context, "error");

  assert.equal(exporter.records.length, 0);
  await engine.shutdown(Context.background());
  assert.equal(exporter.shutdownCalled, true);
  const records = exporter.records;
  assert.equal(records.length, 4);
  assert.deepEqual(
    records.map(({ severityText, severityNumber, body }) => ({
      severityText,
      severityNumber,
      body
    })),
    [
      { severityText: "debug", severityNumber: SeverityNumber.DEBUG, body: "debug" },
      { severityText: "info", severityNumber: SeverityNumber.INFO, body: "info" },
      { severityText: "warn", severityNumber: SeverityNumber.WARN, body: "request failed" },
      { severityText: "error", severityNumber: SeverityNumber.ERROR, body: "error" }
    ]
  );
  const warning = records[2];
  assert.ok(warning);
  assert.equal(warning.resource.attributes[ATTR_SERVICE_NAME], "orderservice");
  assert.deepEqual(warning.attributes, {
    service: "orderservice",
    stream: "Process Order",
    safe: 42,
    exact_unsafe: "9007199254740993",
    ratio: 1.5,
    retry: true,
    error: "timeout",
    details: "[object Object]"
  });
  assert.equal(warning.hrTime[0], 1_787_011_200);
});

await test("OpenTelemetry logs correlate only through the explicit MessageContext", async () => {
  const logExporter = new RecordingExporter();
  const spanExporter = new RecordingSpanExporter();
  const tracing = new opentelemetry.OpenTelemetryTracingEngine({
    serviceName: "orderservice",
    exporter: spanExporter
  });
  const logs = new opentelemetry.OpenTelemetryLogsEngine({
    serviceName: "orderservice",
    exporter: logExporter,
    batch: { scheduledDelayMillis: 60_000 }
  });
  const started = tracing
    .tracing()
    .tracer("orderservice")
    .start(new MessageContext(), "stream.map");

  logs.defaultLogger().info(started.context, "inside span");
  logs.defaultLogger().info(new MessageContext(), "background");
  started.span.end();
  await logs.shutdown(Context.background());
  await tracing.shutdown(Context.background());

  const correlated = logExporter.records[0];
  const background = logExporter.records[1];
  assert.ok(correlated);
  assert.ok(background);
  assert.ok(correlated.spanContext);
  assert.equal(correlated.spanContext.traceId, started.span.spanContext().traceId);
  assert.equal(correlated.spanContext.spanId, started.span.spanContext().spanId);
  assert.equal(background.spanContext, undefined);
});

await test("OpenTelemetry log shutdown obeys cancellation without leaking listeners", async () => {
  let resolveShutdown: (() => void) | undefined;
  class BlockingExporter extends RecordingExporter {
    public override shutdown(): Promise<void> {
      this.shutdownCalled = true;
      return new Promise<void>((resolve) => {
        resolveShutdown = resolve;
      });
    }
  }
  const exporter = new BlockingExporter();
  const engine = new opentelemetry.OpenTelemetryLogsEngine({
    serviceName: "orderservice",
    exporter
  });
  const controller = new AbortController();
  const shutdown = engine.shutdown(new Context(controller.signal));
  controller.abort(new Error("deadline exceeded"));
  await assert.rejects(shutdown, /deadline exceeded/);
  resolveShutdown?.();
  await engine.shutdown(Context.background());
});

await test("OpenTelemetry logs do not fail application logging when the collector is unavailable", async () => {
  const engine = new opentelemetry.OpenTelemetryLogsEngine({
    serviceName: "orderservice",
    endpoint: "http://127.0.0.1:1",
    exportTimeoutMillis: 50,
    batch: {
      scheduledDelayMillis: 1,
      exportTimeoutMillis: 50,
      maxExportBatchSize: 1
    }
  });
  assert.doesNotThrow(() => {
    engine.defaultLogger().info(new MessageContext(), "collector unavailable");
  });
  await assert.doesNotReject(engine.shutdown(Context.background()));
});
