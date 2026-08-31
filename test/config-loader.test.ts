import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  type CanonicalConfig,
  applyEnvironment,
  Context,
  deepMerge,
  loadRuntimeConfig,
  RuntimeConfig,
  RuntimeConfigLoader,
  RuntimeConfigStore
} from "@gorundebug/tsservicelib/runtime";
import { TestLog } from "@gorundebug/tsservicelib/runtime/testlog";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

function config(revision: number): CanonicalConfig {
  return {
    services: [
      {
        id: 1,
        name: "Order Service",
        color: "#000000",
        environment: "test",
        grpcHost: "127.0.0.1",
        grpcPort: 9201,
        httpHost: "127.0.0.1",
        httpPort: 9091,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment",
        properties: {}
      }
    ],
    streams: [],
    dataConnectors: [],
    endpoints: [],
    pools: [],
    links: [],
    modules: [],
    types: [],
    properties: { revision }
  };
}

// Node's test runner executes files concurrently; transport and event-loop
// stress suites can delay this deliberately short polling loop on loaded CI
// hosts. The loader still polls every 5 ms, while this bound prevents a false
// failure caused only by runner scheduling.
async function waitUntil(predicate: () => boolean, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("condition did not become true");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

await test("config merge clones defaults and applies overlays before typed environment patches", () => {
  const defaults = { service: { host: "default", ports: [1, 2], timeout: 100 } };
  const base = { service: { host: "base" } };
  const values = { service: { timeout: 200 } };
  const merged = deepMerge(deepMerge(defaults, base), values);
  applyEnvironment(
    merged,
    [{ environment: "SERVICE_TIMEOUT", path: ["service", "timeout"], parse: Number }],
    { SERVICE_TIMEOUT: "300" }
  );

  assert.deepEqual(merged, { service: { host: "base", ports: [1, 2], timeout: 300 } });
  assert.deepEqual(defaults, { service: { host: "default", ports: [1, 2], timeout: 100 } });
  const service = merged.service as { readonly ports: number[] };
  assert.notEqual(service.ports, defaults.service.ports);
  service.ports.push(3);
  assert.deepEqual(defaults.service.ports, [1, 2]);
});

await test("config loader applies Docker overrides after user values and before environment", async () => {
  const directory = await mkdtemp(join(tmpdir(), "tsservicelib-config-layers-"));
  const configPath = join(directory, "config.yaml");
  const valuesPath = join(directory, "values.yaml");
  const overridesPath = join(directory, "docker.yaml");
  try {
    await writeFile(configPath, "properties:\n  timeout: 0\n  address: localhost\n");
    await writeFile(valuesPath, "properties:\n  timeout: 5000\n");
    await writeFile(overridesPath, "properties:\n  address: inventoryservice\n");
    const loaded = await loadRuntimeConfig({
      configPath,
      valuesPath,
      overridesPath,
      environment: { SERVICE_TIMEOUT: "6000" },
      patches: [{ environment: "SERVICE_TIMEOUT", path: ["properties", "timeout"], parse: Number }],
      schema: {
        parse: (value: unknown) => ({
          ...config(0),
          properties: (value as { properties: Record<string, unknown> }).properties
        })
      }
    });

    assert.deepEqual(loaded.config().properties, {
      timeout: 6000,
      address: "inventoryservice"
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

await test("config loader publishes stable snapshots and retains the last valid one", async () => {
  const directory = await mkdtemp(join(tmpdir(), "tsservicelib-config-loader-"));
  const valuesPath = join(directory, "overrides.yaml");
  const runtimeOverridesPath = join(directory, "runtime-overrides.yaml");
  const metrics = new TestMetrics();
  const logs = new TestLog();
  const store = new RuntimeConfigStore(new RuntimeConfig(config(0)));
  await writeFile(valuesPath, "1\n");
  await writeFile(runtimeOverridesPath, "0\n");
  const loader = new RuntimeConfigLoader({
    paths: [valuesPath, runtimeOverridesPath],
    pollIntervalMs: 5,
    store,
    service: "Order Service",
    metrics,
    logger: logs.defaultLogger(),
    async load(): Promise<RuntimeConfig> {
      const value = (await readFile(valuesPath, "utf8")).trim();
      const runtimeOverride = (await readFile(runtimeOverridesPath, "utf8")).trim();
      if (value === "invalid") throw new Error("invalid config");
      return new RuntimeConfig(config(Number(value) + Number(runtimeOverride)));
    }
  });

  try {
    await loader.start(Context.background());
    assert.equal(store.current().config().properties["revision"], 1);

    await writeFile(valuesPath, "2\n");
    await waitUntil(() => store.current().config().properties["revision"] === 2);
    assert.equal(
      metrics.counterValue("service_config_reloads_total", {
        service: "Order Service",
        event: "success"
      }),
      1
    );

    await writeFile(runtimeOverridesPath, "10\n");
    await waitUntil(() => store.current().config().properties["revision"] === 12);
    assert.equal(
      metrics.counterValue("service_config_reloads_total", {
        service: "Order Service",
        event: "success"
      }),
      2
    );

    await writeFile(valuesPath, "invalid\n");
    await waitUntil(
      () =>
        metrics.counterValue("service_config_reloads_total", {
          service: "Order Service",
          event: "error"
        }) === 1
    );
    assert.equal(store.current().config().properties["revision"], 12);
    assert.equal(logs.entriesAtLevel("error").length, 1);

    await writeFile(valuesPath, "3\n");
    await waitUntil(() => store.current().config().properties["revision"] === 13);
    assert.equal(
      metrics.counterValue("service_config_reloads_total", {
        service: "Order Service",
        event: "success"
      }),
      3
    );

    await loader.stop(Context.background());
    await writeFile(valuesPath, "4\n");
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(store.current().config().properties["revision"], 13);
    await loader.stop(Context.background());
  } finally {
    await loader.stop(Context.background());
    await rm(directory, { recursive: true, force: true });
  }
});
