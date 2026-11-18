import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseCanonicalConfig,
  requireRabbitMQDataConnectorConfig,
  requireRabbitMQEndpointConfig,
  validateRabbitMQEndpoint,
  type RabbitMQDataConnectorConfig,
  type RabbitMQEndpointConfig
} from "@gorundebug/tsservicelib/runtime/config";
const base: RabbitMQDataConnectorConfig = {
  id: 1,
  name: "broker",
  type: 9,
  implementation: "rabbitmq-amqp-js-client",
  properties: {},
  servers: ["amqp://localhost:5672"],
  connectTimeoutMs: 2000,
  operationTimeoutMs: 5000
};
await test("RabbitMQ canonical defaults preserve false flags and native fields outside properties", () => {
  const parsed = parseCanonicalConfig({
    dataConnectors: {
      broker: {
        id: 1,
        name: "broker",
        type: 9,
        implementation: base.implementation,
        servers: base.servers,
        virtualHost: "/",
        business: "kept"
      }
    },
    endpoints: {
      input: {
        id: 2,
        name: "input",
        idDataConnector: 1,
        queueName: "events",
        enabled: false,
        createQueue: false,
        functionName: "Events"
      }
    }
  });
  const c = requireRabbitMQDataConnectorConfig(parsed.dataConnectors[0]),
    e = requireRabbitMQEndpointConfig(parsed.endpoints[0]);
  assert.equal(c.connectTimeoutMs, 2000);
  assert.equal(c.operationTimeoutMs, 5000);
  assert.equal(c.virtualHost, "/");
  assert.equal(c.properties["virtualHost"], undefined);
  assert.equal(c.properties["business"], "kept");
  assert.equal(e.enabled, false);
  assert.equal(e.createQueue, false);
  assert.equal(e.createExchange, false);
  assert.equal(e.createBinding, false);
  assert.equal(e.consumerCredits, undefined);
  assert.equal(e.publisherMaxInFlight, undefined);
  assert.equal(e.properties["queueName"], undefined);
  validateRabbitMQEndpoint(e, false);
});
for (const [name, patch] of Object.entries({
  zero: { connectTimeoutMs: 0 },
  fractional: { operationTimeoutMs: 0.5 },
  timerOverflow: { connectTimeoutMs: 2147483648 },
  empty: { servers: [] },
  inlineSecret: { servers: ["amqp://user:SECRET@host"] },
  slash: { servers: ["amqp://host/"] },
  query: { servers: ["amqp://host?"] },
  fragment: { servers: ["amqp://host#"] },
  space: { servers: ["amqp://host "] },
  port: { servers: ["amqp://host:0"] },
  protocol: { servers: ["http://host"] },
  keypair: { tlsEnabled: true, tlsCertFile: "/cert" },
  tls: { tlsCaFile: "/ca" },
  mixed: { servers: ["amqps://host", "amqp://host"] },
  token: { token: "SECRET" },
  credentialFile: { credentialsFile: "/creds" },
  partialAuth: { password: "SECRET" },
  virtualHost: { virtualHost: "other" },
  serverName: { tlsEnabled: true, tlsServerName: "other" },
  implementation: { implementation: "amqplib" }
}))
  await test(`RabbitMQ rejects ${name} without secret leakage`, () => {
    assert.throws(
      () => requireRabbitMQDataConnectorConfig({ ...base, ...patch }),
      (error) => error instanceof Error && !error.message.includes("SECRET")
    );
  });
const ep: RabbitMQEndpointConfig = {
  id: 2,
  name: "input",
  idDataConnector: 1,
  properties: {},
  enabled: true,
  queueName: "events",
  createQueue: false,
  createExchange: false,
  createBinding: false
};
for (const patch of [
  { consumerCredits: 0 },
  { consumerCredits: 4097 },
  { consumerCredits: 1.1 },
  { publisherMaxInFlight: 1 },
  { exchangeType: "direct" as const },
  { createBinding: true },
  { queueName: undefined },
  { queueType: "invalid" as "classic" }
])
  await test(`RabbitMQ rejects endpoint ${JSON.stringify(patch)}`, () => {
    assert.throws(() => {
      validateRabbitMQEndpoint({ ...ep, ...patch }, false);
    });
  });
await test("RabbitMQ TLS and explicit creation accepted without shared interface changes", () => {
  requireRabbitMQDataConnectorConfig({
    ...base,
    servers: ["amqps://localhost"],
    tlsCaFile: "/ca",
    username: "runtime",
    password: "runtime"
  });
  validateRabbitMQEndpoint(
    {
      ...ep,
      consumerCredits: 1,
      createQueue: true,
      exchangeName: "events",
      exchangeType: "topic",
      createExchange: true,
      createBinding: true,
      routingKey: "event.*"
    },
    false
  );
});
