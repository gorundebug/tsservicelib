import assert from "node:assert/strict";
import { resolve } from "node:path";
import { test } from "node:test";
import { pathToFileURL } from "node:url";

import { Context as ActivityContext } from "@temporalio/activity";
import { NativeConnection, Runtime, Worker, type WorkerOptions } from "@temporalio/worker";

import { TemporalConnector } from "@gorundebug/tsservicelib/datasource/temporal";
import {
  MessageContext,
  noopMetrics,
  type RuntimeEnvironment
} from "@gorundebug/tsservicelib/runtime";

import type * as Propagation from "../src/datasource/temporal/context-propagation.js";

const propagation = (await import(
  pathToFileURL(resolve("dist/datasource/temporal/context-propagation.js")).href
)) as typeof Propagation;

// Exercise the Activity closure actually handed to Worker.create, without a
// live server or an alternative public handler/identity API.
await test("remote Activity uses receiver-local endpoint ID and retains business identity", async (t) => {
  t.mock.method(Runtime, "install", () => undefined);
  t.mock.method(
    NativeConnection,
    "connect",
    async () => ({ close: async () => undefined }) as unknown as NativeConnection
  );
  const cancellationSignal = new AbortController().signal;
  const activityContext = {
    cancellationSignal,
    info: { startToCloseTimeoutMs: 0 }
  } as unknown as ActivityContext;
  t.mock.method(ActivityContext, "current", () => activityContext);
  let activities: Record<string, (...args: unknown[]) => unknown> | undefined;
  let finish: (() => void) | undefined;
  const running = new Promise<void>((resolve) => {
    finish = resolve;
  });
  t.mock.method(Worker, "create", async (options: WorkerOptions) => {
    activities = options.activities as Record<string, (...args: unknown[]) => unknown> | undefined;
    return { run: () => running, shutdown: () => finish?.() } as unknown as Worker;
  });
  const endpoint = {
    id: 7,
    name: "Durable Job",
    idDataConnector: 1,
    enabled: true,
    temporalExecutionType: "Activity",
    taskQueue: "external",
    schedule: "",
    maxConcurrentActivities: 1,
    maxConcurrentWorkflowTasks: 1
  };
  const connectorConfig = {
    id: 1,
    name: "Temporal",
    type: 6,
    implementation: "temporal/typescript",
    address: "not-connected:7233",
    namespace: "default",
    identity: "",
    apiKey: "",
    tlsEnabled: false,
    tlsServerName: "",
    tlsCaFile: "",
    tlsCertFile: "",
    tlsKeyFile: "",
    workerStopTimeout: 1000
  };
  const environment = {
    runtimeConfig: () => ({
      dataConnectorById: () => connectorConfig,
      endpointById: () => endpoint
    }),
    serviceConfig: () => ({ shutdownTimeout: 1000 }),
    metrics: () => noopMetrics,
    tracing: () => undefined
  } as unknown as RuntimeEnvironment;
  const connector = new TemporalConnector(1, environment);
  let calls = 0;
  connector.registerEndpoint(7, async (envelope) => {
    calls += 1;
    assert.equal(envelope.endpointId, 7);
    assert.equal(envelope.messageId, "job-1");
    assert.equal(envelope.streamId, "stream-1");
    return { payload: envelope.payload };
  });
  const context = new MessageContext();
  await connector.start(context);
  try {
    await connector.startAdmission(context);
    const invoke = activities?.["temporal.endpoint.durable_job.v1"];
    assert.ok(invoke);
    const inbound = propagation.temporalActivityInterceptors(activityContext, false).inbound;
    assert.ok(inbound?.execute);
    for (const endpointId of [7, 42]) {
      const wire = {
        version: 1,
        endpointId,
        messageId: "job-1",
        streamId: "stream-1",
        payload: [1, 2, 3]
      };
      const result: unknown = await inbound.execute({ args: [wire], headers: {} }, () =>
        Promise.resolve(invoke(wire))
      );
      assert.deepEqual(result, { payload: [1, 2, 3] });
      assert.equal(wire.endpointId, endpointId);
    }
    await assert.rejects(async () =>
      invoke({ version: 2, endpointId: 42, messageId: "job-1", payload: [] })
    );
    await assert.rejects(async () =>
      invoke({ version: 1, endpointId: 42, messageId: "", payload: [] })
    );
    assert.equal(calls, 2);
  } finally {
    await connector.stop(context);
  }
});
