import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type CanonicalConfig,
  RuntimeConfig,
  RuntimeConfigStore
} from "@gorundebug/tsservicelib/runtime/config";
import {
  Context,
  NoopMetricsEngine,
  ServiceApp,
  noopLogger
} from "@gorundebug/tsservicelib/runtime";

function canonicalConfig(): CanonicalConfig {
  return {
    services: [
      {
        id: 1,
        name: "orderservice",
        color: "#000000",
        environment: "development",
        grpcHost: "0.0.0.0",
        grpcPort: 9090,
        httpHost: "0.0.0.0",
        httpPort: 9091,
        metricsHandler: "/metrics",
        shutdownTimeout: 5_000,
        statusHandler: "/status/data",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment",
        properties: {}
      }
    ],
    streams: [
      {
        id: 10,
        name: "processOrder",
        type: "Input",
        pipeline: "request",
        idService: 1,
        idSource: 0,
        idSources: [],
        xPos: 0,
        yPos: 0,
        properties: {}
      }
    ],
    dataConnectors: [
      {
        id: 20,
        name: "orderServiceApi",
        type: 4,
        implementation: "grpcJs",
        properties: {}
      }
    ],
    endpoints: [
      {
        id: 30,
        name: "processOrder",
        idDataConnector: 20,
        properties: {}
      }
    ],
    pools: [],
    links: [],
    modules: [{ name: "model", path: "example-model", properties: {} }],
    types: [],
    properties: {}
  };
}

await test("runtime config preserves canonical field names and indexes", () => {
  const runtime = new RuntimeConfig(canonicalConfig());

  assert.equal(runtime.serviceById(1)?.name, "orderservice");
  assert.equal(runtime.streamByName("processOrder")?.idService, 1);
  assert.equal(runtime.dataConnectorById(20)?.name, "orderServiceApi");
  assert.equal(runtime.endpointById(30)?.name, "processOrder");
  assert.equal(runtime.moduleByName("model")?.path, "example-model");
});

await test("runtime config deeply freezes every published snapshot", () => {
  const source = canonicalConfig();
  const runtime = new RuntimeConfig(source);
  const service = runtime.config().services[0];
  assert.ok(service);
  assert.equal(Object.isFrozen(runtime.config()), true);
  assert.equal(Object.isFrozen(runtime.config().services), true);
  assert.equal(Object.isFrozen(service), true);
  assert.equal(Object.isFrozen(service.properties), true);
  assert.throws(() => {
    Object.assign(service.properties, { changed: true });
  }, TypeError);
});

await test("runtime config rejects duplicate identities and broken references", () => {
  const duplicate = canonicalConfig();
  const service = duplicate.services[0];
  assert.ok(service);
  assert.throws(
    () =>
      new RuntimeConfig({
        ...duplicate,
        services: [...duplicate.services, { ...service }]
      }),
    /duplicate service name/
  );

  const broken = canonicalConfig();
  const stream = broken.streams[0];
  assert.ok(stream);
  assert.throws(
    () =>
      new RuntimeConfig({
        ...broken,
        streams: [{ ...stream, idService: 404 }]
      }),
    /references missing service id 404/
  );
});

await test("runtime config rejects mismatched endpoint transports, pools and ranges", () => {
  const base = canonicalConfig();
  const endpoint = base.endpoints[0];
  const service = base.services[0];
  assert.ok(endpoint);
  assert.ok(service);
  assert.throws(
    () =>
      new RuntimeConfig({
        ...base,
        endpoints: [{ ...endpoint, httpMethodType: "POST", path: "/v1/order" }]
      }),
    /type does not match data connector/
  );
  assert.throws(
    () =>
      new RuntimeConfig({
        ...base,
        services: [{ ...service, httpPort: 70_000 }]
      }),
    /httpPort must be an integer between 0 and 65535/
  );
  assert.throws(
    () =>
      new RuntimeConfig({
        ...base,
        links: [
          {
            from: 10,
            to: 10,
            callSemantics: { taskPool: { poolName: "missing" } },
            properties: {}
          }
        ]
      }),
    /references missing pool missing/
  );
  assert.throws(
    () =>
      new RuntimeConfig({
        ...base,
        pools: [{ name: "workers", executorsCount: 0, queueCapacity: 0, properties: {} }]
      }),
    /executorsCount must be a positive integer/
  );

  assert.throws(
    () =>
      new RuntimeConfig({
        ...base,
        dataConnectors: [
          {
            id: 20,
            name: "inventoryServiceApi",
            type: 2,
            implementation: "grpc/grpc-js",
            address: "dns:///inventoryservice:9202",
            connectionsCount: 0,
            properties: {}
          }
        ],
        endpoints: [
          {
            ...endpoint,
            grpcMethodType: "NoStreaming",
            methodName: "ProcessOrder"
          }
        ]
      }),
    /connectionsCount must be a positive integer/
  );
});

await test("runtime config requires DurableCall to reference a Temporal connector", () => {
  const base = canonicalConfig();
  const temporal = {
    id: 21,
    name: "temporal",
    type: 6 as const,
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
    maxConcurrentActivities: 8,
    maxConcurrentWorkflows: 4,
    properties: {}
  };
  assert.doesNotThrow(
    () =>
      new RuntimeConfig({
        ...base,
        dataConnectors: [...base.dataConnectors, temporal],
        links: [
          {
            from: 10,
            to: 10,
            callSemantics: {
              durableCall: {
                idDataConnector: temporal.id,
                taskQueue: "automation",
                workflowExecutionTimeout: 0,
                activityStartToCloseTimeout: 30_000,
                activityHeartbeatTimeout: 0,
                maximumAttempts: 3
              }
            },
            properties: {}
          }
        ]
      })
  );
  assert.throws(
    () =>
      new RuntimeConfig({
        ...base,
        links: [
          {
            from: 10,
            to: 10,
            callSemantics: {
              durableCall: {
                idDataConnector: 20,
                taskQueue: "automation",
                workflowExecutionTimeout: 0,
                activityStartToCloseTimeout: 30_000,
                activityHeartbeatTimeout: 0,
                maximumAttempts: 3
              }
            },
            properties: {}
          }
        ]
      }),
    /requires a Temporal data connector/
  );
});

await test("reload retains the last valid snapshot and publishes a valid one once", async () => {
  const initial = new RuntimeConfig(canonicalConfig());
  const store = new RuntimeConfigStore(initial);
  let published = 0;

  assert.equal(
    await store.reload(
      () => Promise.reject(new Error("invalid")),
      () => {
        published += 1;
      }
    ),
    false
  );
  assert.equal(store.current(), initial);
  assert.equal(published, 0);

  const next = new RuntimeConfig({
    ...canonicalConfig(),
    properties: { revision: 2 }
  });
  assert.equal(
    await store.reload(
      () => Promise.resolve(next),
      () => {
        published += 1;
      }
    ),
    true
  );
  assert.equal(store.current(), next);
  assert.equal(published, 1);
});

await test("published pool sizes resize the existing runtime pools", async () => {
  const initial = canonicalConfig();
  const service = initial.services[0];
  assert.ok(service !== undefined);
  const poolConfig = { name: "workers", executorsCount: 1, queueCapacity: 0, properties: {} };
  const priorityPoolConfig = {
    name: "priority-workers",
    executorsCount: 2,
    queueCapacity: 0,
    properties: {}
  };
  const withPool: CanonicalConfig = {
    ...initial,
    services: [
      {
        ...service,
        defaultCallSemantics: { taskPool: { poolName: "workers" } }
      }
    ],
    pools: [poolConfig, priorityPoolConfig],
    links: [
      {
        from: 10,
        to: 10,
        callSemantics: {
          priorityTaskPool: { poolName: "priority-workers", priority: 10 }
        },
        properties: {}
      }
    ]
  };
  const store = new RuntimeConfigStore(new RuntimeConfig(withPool));
  const app = new ServiceApp(store, 1, {
    logger: noopLogger,
    metricsEngine: new NoopMetricsEngine()
  });
  const pool = app.environment().taskPool("workers");
  const priorityPool = app.environment().priorityTaskPool("priority-workers");
  assert.ok(pool !== undefined);
  assert.ok(priorityPool !== undefined);
  assert.equal(pool.executorsCount(), 1);
  assert.equal(priorityPool.executorsCount(), 2);

  store.publish(
    new RuntimeConfig({
      ...withPool,
      pools: [
        { ...poolConfig, executorsCount: 3 },
        { ...priorityPoolConfig, executorsCount: 4 }
      ]
    })
  );
  assert.equal(pool.executorsCount(), 3);
  assert.equal(priorityPool.executorsCount(), 4);

  const published = store.current();
  assert.throws(() => {
    store.publish(
      new RuntimeConfig({
        ...withPool,
        services: [{ ...service, defaultCallSemantics: undefined }],
        pools: [],
        links: []
      })
    );
  }, /pool config workers not found/);
  assert.equal(store.current(), published);
  assert.equal(pool.executorsCount(), 3);
  assert.equal(priorityPool.executorsCount(), 4);

  await app.stop(Context.background());
});
