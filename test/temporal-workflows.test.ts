import assert from "node:assert/strict";
import { resolve } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

await test("Temporal workflow bundle exports the stable cross-language contract names", async () => {
  const moduleUrl = pathToFileURL(resolve("dist/datasource/temporal/workflows.js")).href;
  const workflows: typeof import("../src/datasource/temporal/workflows.js") = await import(moduleUrl);
  assert.equal(workflows["servicegen.durable-link.v1"], workflows.servicegenDurableLinkV1);
  assert.equal(
    workflows["servicegen.temporal-endpoint.v1"],
    workflows.servicegenTemporalEndpointV1
  );
});

await test("scheduled time uses the Temporal Schedule workflow ID suffix", async () => {
  const moduleUrl = pathToFileURL(resolve("dist/datasource/temporal/scheduled-time.js")).href;
  const {
    scheduledTimeFromWorkflowId
  }: typeof import("../src/datasource/temporal/scheduled-time.js") = await import(moduleUrl);
  const fallback = new Date("2026-08-24T12:35:01Z");
  assert.equal(
    scheduledTimeFromWorkflowId("servicegen/schedule/1/3-2026-08-24T12:30:00.123Z", fallback),
    Date.parse("2026-08-24T12:30:00.123Z")
  );
  assert.equal(scheduledTimeFromWorkflowId("manual-workflow", fallback), fallback.getTime());
});
