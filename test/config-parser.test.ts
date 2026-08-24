import assert from "node:assert/strict";
import { test } from "node:test";

import {
  parseCanonicalConfig,
  requireGrpcDataConnectorConfig,
  requireHttpDataConnectorConfig,
  requireHttpEndpointConfig,
  requireInputStreamConfig,
  requireMapStreamConfig
} from "@gorundebug/tsservicelib/runtime/config";

function canonicalDocument(
  overrides: { readonly serviceId?: unknown; readonly transformation?: unknown } = {}
): Record<string, unknown> {
  return {
    services: {
      orderService: {
        id: overrides.serviceId ?? 3,
        name: "Order Service",
        color: "#ff5c00",
        defaultCallSemantics: 3,
        environment: "production",
        grpcHost: "0.0.0.0",
        grpcPort: 9201,
        httpHost: "0.0.0.0",
        httpPort: 9091,
        golangVersion: "1.25.4",
        metricsHandler: "metrics",
        programmingLanguage: 6,
        shutdownTimeout: 30_000,
        statusHandler: "status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment",
        customServiceProperty: "retained"
      }
    },
    streams: {
      processOrder: {
        id: 12,
        name: "Process Order",
        type: overrides.transformation ?? 1,
        pipeline: "order",
        idService: 3,
        idSource: 0,
        xPos: -760,
        yPos: -367,
        valueType: "Order",
        idEndpoint: 3
      },
      mapOrder: {
        id: 13,
        name: "Map Order",
        type: 2,
        pipeline: "order",
        idService: 3,
        idSource: 12,
        xPos: 0,
        yPos: 0,
        valueType: "OrderState",
        functionName: "MapOrder"
      }
    },
    dataConnectors: {
      orderServiceApi: {
        id: 3,
        name: "Order Service API",
        type: 1,
        implementation: "node/http",
        module: "order_service_api",
        useDedicatedListener: false
      }
    },
    endpoints: {
      processOrder: {
        id: 3,
        name: "Process Order",
        idDataConnector: 3,
        httpMethodType: "POST",
        path: "/v1/processorder",
        functionName: "ProcessOrder"
      }
    },
    pools: {
      defaultPool: { name: "Default Pool", executorsCount: 2 }
    },
    links: {
      processOrderToMapOrder: {
        from: 12,
        to: 13,
        callSemantics: 4,
        poolName: "Default Pool",
        priority: 1,
        customLinkProperty: true
      }
    },
    modules: {
      model: { name: "model", path: "github.com/gorundebug/example-model" }
    },
    types: {
      order: {
        name: "Order",
        type: "struct",
        definitionFormat: 1,
        publicType: false,
        transferByValue: false,
        useAlias: false
      }
    },
    customRootProperty: 42
  };
}

await test("canonical config parser normalizes named sections without losing extensions", () => {
  const config = parseCanonicalConfig(canonicalDocument());

  const service = config.services[0];
  const link = config.links[0];
  assert.ok(service);
  assert.ok(link);
  assert.equal(service.id, 3);
  assert.deepEqual(service.defaultCallSemantics, {
    taskPool: { poolName: "" }
  });
  assert.equal(service.properties["customServiceProperty"], "retained");
  assert.equal(config.streams[0]?.name, "Map Order");
  assert.equal(config.streams[1]?.type, "Input");
  assert.equal(requireMapStreamConfig(config.streams[0]).valueType, "OrderState");
  assert.equal(requireInputStreamConfig(config.streams[1]).idEndpoint, 3);
  assert.equal(config.dataConnectors[0]?.type, 1);
  assert.equal(
    requireHttpDataConnectorConfig(config.dataConnectors[0]).module,
    "order_service_api"
  );
  assert.equal("enabled" in (config.endpoints[0] ?? {}), false);
  assert.equal(requireHttpEndpointConfig(config.endpoints[0]).path, "/v1/processorder");
  assert.deepEqual(link.callSemantics, {
    priorityTaskPool: { poolName: "Default Pool", priority: 1 }
  });
  assert.equal(link.properties["customLinkProperty"], true);
  assert.equal(service.golangVersion, "1.25.4");
  assert.equal(service.properties["programmingLanguage"], 6);
  assert.equal(config.modules[0]?.path, "github.com/gorundebug/example-model");
  assert.equal(config.types[0]?.definitionFormat, 1);
  assert.equal(config.properties["customRootProperty"], 42);
});

await test("canonical config parser rejects invalid identities and enum values at the boundary", () => {
  assert.throws(
    () => parseCanonicalConfig(canonicalDocument({ serviceId: "3" })),
    /services\.orderService\.id/
  );
  assert.throws(
    () => parseCanonicalConfig(canonicalDocument({ transformation: 99 })),
    /unknown transformation type/
  );
  const valid = parseCanonicalConfig(canonicalDocument());
  assert.throws(() => requireMapStreamConfig(valid.streams[1]), /invalid Map stream config/);
});

await test("gRPC connector defaults connectionsCount to one", () => {
  const document = canonicalDocument();
  document["dataConnectors"] = {
    inventoryServiceApi: {
      id: 3,
      name: "Inventory Service API",
      type: 2,
      implementation: "grpc/grpc-js",
      address: "dns:///inventoryservice:9202"
    }
  };
  document["endpoints"] = {
    processOrder: {
      id: 3,
      name: "Process Order",
      idDataConnector: 3,
      grpcMethodType: "NoStreaming",
      methodName: "ProcessOrder"
    }
  };

  const config = parseCanonicalConfig(document);
  assert.equal(requireGrpcDataConnectorConfig(config.dataConnectors[0]).connectionsCount, 1);
});

await test("Cron, Temporal and DurableCall documents normalize without losing policy", () => {
  const document = canonicalDocument();
  document["dataConnectors"] = {
    temporal: {
      id: 7,
      name: "Temporal",
      type: 6,
      implementation: "temporal/typescript",
      address: "temporal:7233",
      namespace: "default",
      maxConcurrentActivities: 8,
      maxConcurrentWorkflows: 4
    }
  };
  document["endpoints"] = {
    scheduledJob: {
      id: 8,
      name: "Scheduled Job",
      idDataConnector: 7,
      enabled: true,
      taskQueue: "automation",
      schedule: "*/5 * * * *",
      scheduleId: "scheduled-job",
      timezone: "UTC",
      overlapPolicy: "Skip",
      missedRunPolicy: "FireOnce",
      activityStartToCloseTimeout: 30_000,
      maximumAttempts: 3
    }
  };
  document["links"] = {
    durable: {
      from: 12,
      to: 13,
      callSemantics: 6,
      idDataConnector: 7,
      taskQueue: "automation",
      activityStartToCloseTimeout: 30_000,
      maximumAttempts: 3
    }
  };

  const config = parseCanonicalConfig(document);
  const connector = config.dataConnectors[0];
  const endpoint = config.endpoints[0];
  const link = config.links[0];
  assert.ok(connector && endpoint && link);
  assert.equal(connector.type, 6);
  assert("namespace" in connector);
  assert.equal(connector.namespace, "default");
  assert("taskQueue" in endpoint);
  assert.equal(endpoint.taskQueue, "automation");
  assert("overlapPolicy" in endpoint);
  assert.equal(endpoint.overlapPolicy, "Skip");
  assert.deepEqual(link.callSemantics, {
    durableCall: {
      idDataConnector: 7,
      taskQueue: "automation",
      workflowExecutionTimeout: 0,
      activityStartToCloseTimeout: 30_000,
      activityHeartbeatTimeout: 0,
      maximumAttempts: 3
    }
  });
});
