import assert from "node:assert/strict";
import { test } from "node:test";
import { Context } from "../../dist/runtime/context.js";
import { openConnection, wait } from "../../dist/internal/nats/client.js";
import { requireNATSDataConnectorConfig } from "../../dist/runtime/config/nats.js";

await test("NATS completion cannot beat expired context", async () => {
  for (const explicitTimeout of [false, true]) {
    const owner = explicitTimeout ? new Context() : new Context().bounded(5);
    const operation = Promise.resolve().then(() => {
      const end = performance.now() + 15;
      while (performance.now() < end) {
        /* Deliberately delay timer delivery. */
      }
      return "late-success";
    });
    await assert.rejects(wait(operation, owner, explicitTimeout ? 5 : undefined));
  }
});
await test("NATS native startup gate", { skip: !process.env["NATS_STARTUP_URL"] }, async () => {
  const mode = process.env["NATS_STARTUP_MODE"];
  const controller = new AbortController();
  let owner = new Context(controller.signal);
  if (mode === "deadline") owner = owner.bounded(100);
  if (mode === "pre-cancel") controller.abort();
  const timer =
    mode === "cancel"
      ? setTimeout(() => {
          controller.abort();
        }, 40)
      : undefined;
  const config = requireNATSDataConnectorConfig({
    id: 1,
    name: "startup",
    type: 7,
    properties: {},
    implementation: "@nats-io/transport-node",
    servers: [process.env["NATS_STARTUP_URL"] ?? ""],
    connectTimeoutMs: mode === "configured-timeout" ? 100 : 1200,
    operationTimeoutMs: 500
  });
  const begin = performance.now();
  try {
    if (mode === "control") {
      const connection = await openConnection(config, owner);
      await connection.close();
    } else await assert.rejects(openConnection(config, owner));
    assert.ok(performance.now() - begin < 700);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
});
