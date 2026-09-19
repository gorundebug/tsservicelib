/** Explicit real-server integration gate; run after compiling tsconfig.test.json. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";

import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker, bundleWorkflowCode } from "@temporalio/worker";

import type { substreamReplayWorkflow } from "./fixtures/substream-temporal-workflow.js";

const address = process.env["TEMPORAL_ADDRESS"];
assert.ok(address, "TEMPORAL_ADDRESS is required");
const workflowBundle = await bundleWorkflowCode({
  workflowsPath: resolve("dist-test/test/fixtures/substream-temporal-workflow.js"),
  workflowInterceptorModules: [resolve("dist/datasource/temporal/workflow-context-interceptor.js")]
});
const connection = await Connection.connect({ address });
try {
  const nativeConnection = await NativeConnection.connect({ address });
  try {
    const identity = `substream-typescript-${randomUUID()}`;
    const client = new Client({ connection });
    const worker = await Worker.create({
      connection: nativeConnection,
      workflowBundle,
      taskQueue: identity
    });
    const history = await worker.runUntil(async () => {
      const handle = await client.workflow.start<typeof substreamReplayWorkflow>(
        "substreamReplayWorkflow",
        {
          args: [100],
          workflowId: identity,
          taskQueue: identity,
          workflowExecutionTimeout: "60 seconds"
        }
      );
      assert.deepEqual(
        await handle.result(),
        Array.from({ length: 100 }, (_, value) => value * 10)
      );
      return handle.fetchHistory();
    });
    const timers = (history.events ?? []).filter(
      (event) => event.timerFiredEventAttributes != null
    ).length;
    assert.ok(timers >= 100, `Expected durable timer activations, got ${String(timers)}`);
    await Worker.runReplayHistory({ workflowBundle }, history, identity);
    console.log(
      `SUBSTREAM_TEMPORAL_OK calls=100 nested=50 late_results=dropped timers=${String(timers)} replay=ok`
    );
  } finally {
    await nativeConnection.close();
  }
} finally {
  await connection.close();
}
