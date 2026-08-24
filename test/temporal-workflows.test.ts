import assert from "node:assert/strict";
import { resolve } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

await test("Temporal workflow bundle exports the stable cross-language contract names", async () => {
  const moduleUrl = pathToFileURL(resolve("dist/runtime/temporal/workflows.js")).href;
  const workflows: typeof import("../src/runtime/temporal/workflows.js") = await import(moduleUrl);
  assert.equal(workflows["servicegen.durable-link.v1"], workflows.servicegenDurableLinkV1);
  assert.equal(
    workflows["servicegen.temporal-endpoint.v1"],
    workflows.servicegenTemporalEndpointV1
  );
});
