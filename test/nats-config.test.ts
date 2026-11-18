import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseCanonicalConfig,
  requireNATSDataConnectorConfig,
  requireNATSEndpointConfig,
  validateNATSEndpoint
} from "@gorundebug/tsservicelib/runtime/config";

const base = {
  id: 1,
  name: "broker",
  type: 7 as const,
  implementation: "@nats-io/transport-node",
  servers: ["nats://localhost:4222"],
  properties: {},
  connectTimeoutMs: 2000,
  operationTimeoutMs: 5000
};
await test("NATS canonical kinds, defaults, and generic properties stay typed", () => {
  for (const type of [7, 8]) {
    const parsed = parseCanonicalConfig({
      dataConnectors: {
        broker: {
          id: 1,
          name: "broker",
          type,
          implementation: "@nats-io/transport-node",
          servers: base.servers,
          businessSetting: "retained"
        }
      },
      endpoints: {
        input: {
          id: 2,
          name: "input",
          idDataConnector: 1,
          subject: "events",
          functionName: "Events"
        }
      }
    });
    const connector = requireNATSDataConnectorConfig(parsed.dataConnectors[0]);
    assert.equal(connector.implementation, "@nats-io/transport-node");
    assert.equal(connector.connectTimeoutMs, 2000);
    assert.equal(connector.operationTimeoutMs, 5000);
    assert.equal(connector.properties["businessSetting"], "retained");
    assert.equal(connector.properties["servers"], undefined);
    const ep = requireNATSEndpointConfig(parsed.endpoints[0]);
    assert.equal(ep.createStream, false);
    assert.equal(ep.createConsumer, false);
    assert.equal(ep.enabled, false);
  }
});
for (const [name, patch] of Object.entries({
  zero: { connectTimeoutMs: 0 },
  fractional: { operationTimeoutMs: 0.5 },
  overflow: { operationTimeoutMs: 9223372036855 },
  noServers: { servers: [] },
  path: { servers: ["nats://host:4222/path"] },
  slash: { servers: ["nats://host:4222/"] },
  query: { servers: ["nats://host:4222?foo=bar"] },
  emptyQuery: { servers: ["nats://host:4222?"] },
  emptyFragment: { servers: ["nats://host:4222#"] },
  fragment: { servers: ["nats://host:4222#foo"] },
  credentials: { servers: ["nats://user:pass@host:4222"] },
  ws: { servers: ["ws://host"] },
  port: { servers: ["nats://host:0"] },
  tls: { servers: ["tls://host:4222"] },
  uppercaseTls: { servers: ["TLS://host:4222"] },
  tlsFiles: { tlsCaFile: "/mounted/ca.pem" },
  mixed: { servers: ["tls://host:4222", "nats://host:4222"] },
  auth: { token: "test-only", credentialsFile: "/mounted/nats.creds" },
  partialCert: { tlsEnabled: true, tlsCertFile: "/mounted/cert.pem" }
}))
  await test(`NATS rejects invalid ${name}`, () => {
    assert.throws(() => requireNATSDataConnectorConfig({ ...base, ...patch }));
  });
await test("NATS canonical maximum and explicit TLS mode accepted", () => {
  assert.equal(
    requireNATSDataConnectorConfig({ ...base, connectTimeoutMs: 9223372036854 }).connectTimeoutMs,
    9223372036854
  );
  requireNATSDataConnectorConfig({
    ...base,
    tlsEnabled: true,
    servers: ["tls://host:4222", "nats://host:4222"]
  });
});
await test("NATS endpoint mode/direction separation", () => {
  const ep = {
    id: 1,
    name: "events",
    idDataConnector: 1,
    properties: {},
    enabled: true,
    subject: "events.*",
    createStream: false,
    createConsumer: false
  };
  validateNATSEndpoint(ep, 7, false);
  assert.throws(() => {
    validateNATSEndpoint(ep, 7, true);
  });
  assert.throws(() => {
    validateNATSEndpoint({ ...ep, streamName: "EVENTS" }, 7, false);
  });
  assert.throws(() => {
    validateNATSEndpoint(
      { ...ep, streamName: "EVENTS", durableName: "reader", queueGroup: "workers" },
      8,
      false
    );
  });
  assert.throws(() => {
    validateNATSEndpoint({ ...ep, streamName: "EVENTS" }, 8, false);
  });
});
