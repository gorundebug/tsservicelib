import assert from "node:assert/strict";
import { resolve } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { bundleWorkflowCode } from "@temporalio/worker";

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

await test("workflow-safe graph runtime bundles without Node built-ins", async () => {
  const workflowPath = resolve("dist-test/test/fixtures/temporal-workflow-bundle.js");
  const interceptorPath = resolve("dist/datasource/temporal/workflow-context-interceptor.js");
  const tracingInterceptorPath = resolve(
    "node_modules/@temporalio/interceptors-opentelemetry/lib/workflow-interceptors.js"
  );
  const bundle = await bundleWorkflowCode({
    workflowsPath: workflowPath,
    workflowInterceptorModules: [interceptorPath, tracingInterceptorPath]
  });
  assert.match(bundle.code, /workflowSafeRuntimeProbe/u);
  assert.ok(bundle.code.length > 0);
});
