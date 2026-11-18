import assert from "node:assert/strict";
import { test } from "node:test";
import {
  RuntimeConfig,
  parseCanonicalConfig,
  requirePulsarDataConnectorConfig,
  requirePulsarEndpointConfig,
  validatePulsarEndpoint,
  type PulsarDataConnectorConfig,
  type PulsarEndpointConfig
} from "@gorundebug/tsservicelib/runtime/config";
const connector: PulsarDataConnectorConfig = {
  id: 1,
  name: "events",
  type: 11,
  implementation: "pulsar-client-node",
  properties: {},
  servers: ["pulsar://localhost:6650"],
  adminUrl: "http://localhost:8080",
  connectTimeoutMs: 2000,
  operationTimeoutMs: 5000
};
const endpoint: PulsarEndpointConfig = {
  id: 2,
  name: "events-in",
  idDataConnector: 1,
  properties: {},
  enabled: true,
  topic: "persistent://public/default/events",
  subscriptionName: "workers"
};
await test("Pulsar parsing uses connector identity, preserves flags and custom properties", () => {
  const parsed = parseCanonicalConfig({
    dataConnectors: { events: { ...connector, properties: undefined, customValue: "keep" } },
    endpoints: {
      input: { ...endpoint, properties: undefined, createSubscription: false },
      output: {
        id: 3,
        name: "out",
        idDataConnector: 1,
        topic: "persistent://public/default/out",
        createTopic: true,
        publisherMaxInFlight: 16
      }
    }
  });
  assert.doesNotThrow(() => {
    new RuntimeConfig(parsed);
  });
  const c = requirePulsarDataConnectorConfig(parsed.dataConnectors[0]);
  assert.equal(c.adminUrl, connector.adminUrl);
  assert.equal(c.properties["customValue"], "keep");
  assert.equal(c.properties["adminUrl"], undefined);
  const input = requirePulsarEndpointConfig(parsed.endpoints[0]);
  const output = requirePulsarEndpointConfig(parsed.endpoints[1]);
  assert.equal(input.createSubscription, false);
  assert.equal(output.subscriptionName, undefined);
  validatePulsarEndpoint(input, false);
  validatePulsarEndpoint(output, true);
  assert.notEqual(c.servers, connector.servers);
});
for (const [name, patch] of Object.entries({
  missingServer: { servers: [] },
  multipleServers: { servers: ["pulsar://a", "pulsar://b"] },
  credentials: { servers: ["pulsar://u:p@host"] },
  port: { servers: ["pulsar://host:65536"] },
  path: { adminUrl: "http://host/admin" },
  query: { adminUrl: "http://host?secret=x" },
  wrongScheme: { servers: ["redis://host"] },
  tlsDowngrade: { servers: ["pulsar+ssl://host"] },
  keyOnly: { tlsKeyFile: "client.key" },
  mixedAuth: {
    servers: ["pulsar+ssl://host"],
    adminUrl: "https://host",
    tlsCertFile: "cert",
    tlsKeyFile: "key",
    token: "secret"
  },
  timeout: { operationTimeoutMs: 0 },
  communityClient: { implementation: "custom-wrapper" }
}))
  await test(`Pulsar rejects connector ${name}`, () => {
    assert.throws(() => requirePulsarDataConnectorConfig({ ...connector, ...patch }));
  });
for (const [name, patch] of Object.entries({
  relative: { topic: "events" },
  nonPersistent: { topic: "non-persistent://public/default/events" },
  encoded: { topic: "persistent://public/default/a%2fb" },
  missingSubscription: { subscriptionName: "" },
  initialReset: { subscriptionInitialPosition: "Latest" },
  noCreate: { partitions: 2 },
  tooManyPartitions: { partitions: 1001, createTopic: true },
  queueZero: { receiverQueueSize: 0 },
  queueHuge: { receiverQueueSize: 4097 },
  publisherOnSource: { publisherMaxInFlight: 16 }
}))
  await test(`Pulsar rejects endpoint ${name}`, () => {
    assert.throws(() => {
      validatePulsarEndpoint({ ...endpoint, ...patch } as PulsarEndpointConfig, false);
    });
  });
await test("Pulsar accepts all declared source modes and explicit creation position", () => {
  for (const subscriptionType of ["Shared", "Exclusive", "Failover"] as const)
    validatePulsarEndpoint(
      {
        ...endpoint,
        subscriptionType,
        createTopic: true,
        partitions: 2,
        createSubscription: true,
        subscriptionInitialPosition: "Earliest"
      },
      false
    );
  assert.throws(() => {
    validatePulsarEndpoint({ ...endpoint, createSubscription: false }, true);
  });
});
await test("Kafka cannot silently consume Pulsar subscription options", () => {
  assert.throws(
    () =>
      parseCanonicalConfig({
        dataConnectors: {
          k: { id: 1, name: "k", type: 3, implementation: "kafkajs", brokers: "localhost:9092" }
        },
        endpoints: { e: { ...endpoint, subscriptionType: "Shared" } }
      }),
    /requires Pulsar/
  );
});

await test("Pulsar adminToken is typed runtime config with explicit TLS identity conflict", () => {
  const parsed = parseCanonicalConfig({
    dataConnectors: {
      events: { ...connector, properties: undefined, token: "native", adminToken: "admin-secret" }
    }
  });
  const c = requirePulsarDataConnectorConfig(parsed.dataConnectors[0]);
  assert.equal(c.adminToken, "admin-secret");
  assert.equal(c.token, "native");
  assert.equal(c.properties["adminToken"], undefined);
  assert.throws(
    () => requirePulsarDataConnectorConfig({ ...connector, adminToken: "secret\r\nheader" }),
    /adminToken/
  );
  assert.throws(
    () =>
      requirePulsarDataConnectorConfig({
        ...connector,
        servers: ["pulsar+ssl://broker"],
        adminUrl: "https://broker",
        adminToken: "secret",
        tlsCertFile: "cert",
        tlsKeyFile: "key"
      }),
    /token or TLS/
  );
});
