import assert from "node:assert/strict";
import { test } from "node:test";

import { NativeConnection, Runtime } from "@temporalio/worker";

import { TemporalConnector } from "@gorundebug/tsservicelib/datasource/temporal";
import {
  MessageContext,
  noopMetrics,
  type RuntimeEnvironment,
  type TemporalDataConnectorConfig
} from "@gorundebug/tsservicelib/runtime";

await test("Temporal leaves process signals to the service lifecycle with optional SDK metrics", async (t) => {
  const address = process.env["TEMPORAL_SDK_METRICS_BIND_ADDRESS"]?.trim();
  const install = t.mock.method(Runtime, "install", () => undefined);
  const connectionError = new Error("test connection boundary");
  t.mock.method(NativeConnection, "connect", () => Promise.reject(connectionError));
  const config = {
    id: 1,
    name: "temporal",
    type: 6,
    implementation: "temporal/typescript",
    address: "temporal:7233",
    namespace: "default",
    identity: "",
    apiKey: "",
    tlsEnabled: false,
    tlsServerName: "",
    tlsCaFile: "",
    tlsCertFile: "",
    tlsKeyFile: "",
    workerStopTimeout: 1_000,
    properties: {}
  } satisfies TemporalDataConnectorConfig;
  const environment = {
    runtimeConfig: () => ({ dataConnectorById: () => config }),
    serviceConfig: () => ({ shutdownTimeout: 1_000 }),
    metrics: () => noopMetrics,
    tracing: () => undefined
  } as unknown as RuntimeEnvironment;
  for (let index = 0; index < 2; index += 1) {
    const connector = new TemporalConnector(1, environment);
    await assert.rejects(
      connector.start(new MessageContext()),
      (error) => error === connectionError
    );
  }
  assert.equal(install.mock.callCount(), 1);
  assert.deepEqual(install.mock.calls[0]?.arguments, [
    {
      shutdownSignals: [],
      ...(address
        ? {
            telemetryOptions: {
              metrics: { prometheus: { bindAddress: address, useSecondsForDurations: true } }
            }
          }
        : {})
    }
  ]);
});
