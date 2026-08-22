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

await test("service runtime exposes Node event-loop and accepted-work diagnostics", async () => {
  const config: CanonicalConfig = {
    services: [
      {
        id: 1,
        name: "orders",
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
  const metrics = new PrometheusMetricsEngine();
  const app = new ServiceApp(new RuntimeConfigStore(new RuntimeConfig(config)), 1, {
    metricsEngine: metrics
  });

  await app.start(Context.background());
  try {
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
    const output = await metrics.render();
    assert.match(output, /runtime_worker_count\{service="orders"\} 1/u);
    assert.match(output, /runtime_active_work\{service="orders"\} 0/u);
    assert.match(output, /runtime_task_queue_length\{service="orders"\} 0/u);
    assert.match(output, /runtime_active_resources\{service="orders"\} [1-9][0-9]*/u);
    assert.match(output, /runtime_worker_utilization\{service="orders"\} [0-9.e+-]+/u);
    assert.match(output, /runtime_event_loop_lag_seconds\{service="orders"\} [0-9.e+-]+/u);
    assert.match(output, /# TYPE runtime_gc_pause_seconds histogram/u);
  } finally {
    await app.stop(Context.background());
  }
});
