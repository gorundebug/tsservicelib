import assert from "node:assert/strict";
import { test } from "node:test";

import {
  endpointWorkflowId,
  temporalCronExpression
} from "@gorundebug/tsservicelib/datasource/temporal";

await test("Temporal cron preserves portable minute semantics", () => {
  assert.equal(temporalCronExpression("  */5   * * * * "), "0 */5 * * * *");
});

await test("workflow identity uses connector, endpoint and business MessageID", () => {
  assert.equal(
    endpointWorkflowId("Temporal Main", "Durable Job", "order/42:item 7"),
    "temporal_main/endpoint/durable_job/order%2F42%3Aitem%207"
  );
});
