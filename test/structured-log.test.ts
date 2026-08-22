import assert from "node:assert/strict";
import { test } from "node:test";

import {
  bool,
  Context,
  err,
  float64,
  int64,
  LogLevel,
  makeServiceLogsEngine,
  NoopLogsEngine,
  str
} from "@gorundebug/tsservicelib/runtime";
import {
  JsonLogsEngine,
  type JsonLogRecord,
  type JsonLogSink
} from "@gorundebug/tsservicelib/runtime/logging";
import { TestLog } from "@gorundebug/tsservicelib/runtime/testlog";

class RecordingSink implements JsonLogSink {
  public readonly records: JsonLogRecord[] = [];

  public write(record: JsonLogRecord): void {
    this.records.push(record);
  }
}

await test("structured logging preserves the canonical levels and typed fields", () => {
  const engine = new TestLog();
  const logger = engine.defaultLogger();
  const context = Context.background();

  logger.debug(context, "debug event");
  logger.info(context, "info event");
  logger.warn(
    context,
    "request failed",
    str("endpoint", "orders"),
    int64("attempt", 2n),
    float64("ratio", 1.5),
    bool("retry", true)
  );
  logger.error(context, "shutdown failed", err(new Error("timeout")));

  const entries = engine.entries();
  assert.deepEqual(
    entries.map((entry) => entry.level),
    [LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error]
  );
  assert.deepEqual(entries[2]?.fields, [
    { key: "endpoint", type: "string", value: "orders" },
    { key: "attempt", type: "int64", value: 2n },
    { key: "ratio", type: "float64", value: 1.5 },
    { key: "retry", type: "bool", value: true }
  ]);
  const errorField = entries[3]?.fields[0];
  assert.equal(errorField?.type, "error");
  assert.equal(errorField.value instanceof Error, true);
  assert.equal(engine.entriesAtLevel(LogLevel.Error).length, 1);
  engine.reset();
  assert.deepEqual(engine.entries(), []);
});

await test("structured logging writes deterministic JSON-safe production records", () => {
  const sink = new RecordingSink();
  const engine = new JsonLogsEngine({
    sink,
    now: () => new Date("2026-08-18T00:00:00.000Z")
  });
  engine
    .defaultLogger()
    .warn(
      Context.background(),
      "request failed",
      str("service", "orderservice"),
      str("stream", "Process Order"),
      str("endpoint", "orders"),
      int64("attempt", 9_007_199_254_740_993n),
      err(new Error("timeout"))
    );

  assert.deepEqual(sink.records, [
    {
      timestamp: "2026-08-18T00:00:00.000Z",
      severity: "warn",
      message: "request failed",
      fields: {
        service: "orderservice",
        stream: "Process Order",
        endpoint: "orders",
        attempt: "9007199254740993",
        error: "timeout"
      }
    }
  ]);
});

await test("standard benchmark flag selects the no-op logs engine", () => {
  assert.equal(
    makeServiceLogsEngine({ SERVICELIB_NOOP_LOGS: "1" }) instanceof NoopLogsEngine,
    true
  );
  assert.equal(makeServiceLogsEngine({}) instanceof JsonLogsEngine, true);
});
