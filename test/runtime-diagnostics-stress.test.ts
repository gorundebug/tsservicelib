import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type CanonicalConfig,
  Context,
  PrometheusMetricsEngine,
  RuntimeConfig,
  RuntimeConfigStore,
  ServiceApp
} from "@gorundebug/tsservicelib/runtime";

const SERVICE = "event-loop-stress";

await test("runtime diagnostics report event-loop saturation under synchronous work", async () => {
  const metrics = new PrometheusMetricsEngine();
  const app = new ServiceApp(new RuntimeConfigStore(new RuntimeConfig(config())), 1, {
    metricsEngine: metrics
  });

  await app.start(Context.background());
  try {
    // Let monitorEventLoopDelay establish its first sampling interval, then
    // deliberately occupy the single JavaScript event loop long enough to be
    // distinguishable from timer and CI scheduling jitter.
    await delay(30);
    const until = performance.now() + 80;
    while (performance.now() < until) {
      // Intentional event-loop pressure for this acceptance test.
    }
    await delay(30);

    const output = await metrics.render();
    const lag = metric(output, "runtime_event_loop_lag_seconds");
    const utilization = metric(output, "runtime_worker_utilization");
    assert.ok(lag >= 0.04, `expected at least 40ms event-loop lag, received ${String(lag)}s`);
    assert.ok(
      utilization >= 0.3,
      `expected at least 30% event-loop utilization, received ${String(utilization)}`
    );
  } finally {
    await app.stop(Context.background());
  }
});

function config(): CanonicalConfig {
  return {
    services: [
      {
        id: 1,
        name: SERVICE,
        color: "#000000",
        properties: {},
        environment: "test",
        grpcHost: "127.0.0.1",
        grpcPort: 0,
        httpHost: "127.0.0.1",
        httpPort: 0,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment"
      }
    ],
    streams: [],
    dataConnectors: [],
    endpoints: [],
    pools: [],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
}

function metric(output: string, name: string): number {
  const match = new RegExp(`${name}\\{service="${SERVICE}"\\} ([0-9.e+-]+)`, "u").exec(output);
  if (match === null) assert.fail(`metric ${name} is absent from Prometheus output`);
  return Number(match[1]);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
