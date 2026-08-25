import assert from "node:assert/strict";
import { resolve } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import type { servicelibTemporalEndpointV1 } from "../src/datasource/temporal/workflows.js";
import type { scheduledTimeFromWorkflowId as scheduledTime } from "../src/datasource/temporal/scheduled-time.js";

interface WorkflowModule {
  readonly servicelibTemporalEndpointV1: typeof servicelibTemporalEndpointV1;
  readonly "servicelib.temporal-endpoint.v1": typeof servicelibTemporalEndpointV1;
}

await test("Temporal workflow bundle exports the stable cross-language contract names", async () => {
  const moduleUrl = pathToFileURL(resolve("dist/datasource/temporal/workflows.js")).href;
  const loaded: unknown = await import(moduleUrl);
  const workflows = loaded as WorkflowModule;
  assert.equal(
    workflows["servicelib.temporal-endpoint.v1"],
    workflows.servicelibTemporalEndpointV1
  );
});

await test("scheduled time uses the Temporal Schedule workflow ID suffix", async () => {
  const moduleUrl = pathToFileURL(resolve("dist/datasource/temporal/scheduled-time.js")).href;
  const loaded: unknown = await import(moduleUrl);
  const { scheduledTimeFromWorkflowId } = loaded as {
    readonly scheduledTimeFromWorkflowId: typeof scheduledTime;
  };
  const fallback = new Date("2026-08-24T12:35:01Z");
  assert.equal(
    scheduledTimeFromWorkflowId("temporal/schedule/durableJob-2026-08-24T12:30:00.123Z", fallback),
    Date.parse("2026-08-24T12:30:00.123Z")
  );
  assert.equal(scheduledTimeFromWorkflowId("manual-workflow", fallback), fallback.getTime());
});
