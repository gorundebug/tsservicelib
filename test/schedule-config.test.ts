import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type CronDataConnectorConfig,
  type CronEndpointConfig,
  type DataConnectorConfig,
  type EndpointConfig,
  type TemporalDataConnectorConfig,
  type TemporalEndpointConfig,
  requireCronDataConnectorConfig,
  requireCronEndpointConfig,
  requireTemporalDataConnectorConfig,
  requireTemporalEndpointConfig
} from "@gorundebug/tsservicelib/runtime/config";

await test("Cron config guards preserve the generated typed contract", () => {
  const connector = {
    id: 1,
    name: "local cron",
    type: 5,
    implementation: "node/croner",
    properties: {}
  } satisfies CronDataConnectorConfig;
  const endpoint = {
    id: 2,
    name: "tick",
    idDataConnector: 1,
    enabled: true,
    schedule: "*/5 * * * *",
    timezone: "UTC",
    overlapPolicy: "Skip",
    missedRunPolicy: "FireOnce",
    properties: {}
  } satisfies CronEndpointConfig;

  assert.equal(requireCronDataConnectorConfig(connector), connector);
  assert.equal(requireCronEndpointConfig(endpoint).schedule, "*/5 * * * *");
  assert.throws(
    () => requireCronEndpointConfig({ ...endpoint, overlapPolicy: "invalid" } as EndpointConfig),
    /invalid Cron endpoint config/
  );
  assert.throws(
    () => requireCronEndpointConfig({ ...endpoint, timezone: "Europe/Moscow" } as EndpointConfig),
    /invalid Cron endpoint config/
  );
});

await test("Temporal config guards reject incomplete durable transport settings", () => {
  const connector = {
    id: 3,
    name: "temporal",
    type: 6,
    implementation: "temporal/typescript",
    address: "temporal:7233",
    namespace: "default",
    identity: "automation",
    apiKey: "",
    tlsEnabled: false,
    tlsServerName: "",
    tlsCaFile: "",
    tlsCertFile: "",
    tlsKeyFile: "",
    maxConcurrentActivities: 2,
    maxConcurrentWorkflows: 2,
    properties: {}
  } satisfies TemporalDataConnectorConfig;
  const endpoint = {
    id: 4,
    name: "submitted",
    idDataConnector: 3,
    enabled: true,
    schedule: "",
    scheduleId: "",
    timezone: "UTC",
    overlapPolicy: "Skip",
    missedRunPolicy: "Skip",
    taskQueue: "automation",
    temporalExecutionType: "Activity",
    workflowExecutionTimeout: 0,
    activityStartToCloseTimeout: 30_000,
    activityHeartbeatTimeout: 0,
    maximumAttempts: 3,
    properties: {}
  } satisfies TemporalEndpointConfig;

  assert.equal(requireTemporalDataConnectorConfig(connector).namespace, "default");
  assert.equal(requireTemporalEndpointConfig(endpoint).taskQueue, "automation");
  assert.equal(requireTemporalEndpointConfig(endpoint).temporalExecutionType, "Activity");
  assert.throws(
    () =>
      requireTemporalDataConnectorConfig({
        ...connector,
        maxConcurrentActivities: 0
      } as DataConnectorConfig),
    /invalid Temporal data connector config/
  );
  assert.throws(
    () =>
      requireTemporalEndpointConfig({
        ...endpoint,
        activityStartToCloseTimeout: 0
      } as EndpointConfig),
    /invalid Temporal endpoint config/
  );
  assert.equal(
    requireTemporalEndpointConfig({
      ...endpoint,
      temporalExecutionType: "Workflow",
      activityStartToCloseTimeout: 0
    } as EndpointConfig).temporalExecutionType,
    "Workflow"
  );
});
