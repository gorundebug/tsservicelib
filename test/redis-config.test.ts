import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseCanonicalConfig,
  requireRedisStreamsDataConnectorConfig,
  requireRedisStreamsEndpointConfig,
  validateRedisStreamsEndpoint,
  type RedisStreamsDataConnectorConfig,
  type RedisStreamsEndpointConfig
} from "@gorundebug/tsservicelib/runtime/config";
const base: RedisStreamsDataConnectorConfig = {
  id: 1,
  name: "redis",
  type: 10,
  implementation: "node-redis",
  properties: {},
  servers: ["redis://localhost:6379"],
  redisDatabase: 0,
  connectTimeoutMs: 2000,
  operationTimeoutMs: 5000
};
await test("Redis Streams canonical fields, default database/timeouts and explicit false flags", () => {
  const input = {
    dataConnectors: {
      redis: {
        id: 1,
        name: "redis",
        type: 10,
        implementation: "node-redis",
        servers: ["redis://localhost:6379"],
        business: "retained"
      }
    },
    endpoints: {
      input: {
        id: 2,
        name: "input",
        idDataConnector: 1,
        streamKey: "literal*key",
        consumerGroup: "workers",
        createConsumerGroup: false
      }
    }
  };
  const parsed = parseCanonicalConfig(input),
    dc = requireRedisStreamsDataConnectorConfig(parsed.dataConnectors[0]),
    ep = requireRedisStreamsEndpointConfig(parsed.endpoints[0]);
  assert.equal(dc.redisDatabase, 0);
  assert.equal(dc.connectTimeoutMs, 2000);
  assert.equal(dc.operationTimeoutMs, 5000);
  assert.equal(dc.properties["business"], "retained");
  assert.equal(ep.properties["streamKey"], undefined);
  assert.equal(ep.createConsumerGroup, false);
  assert.equal(ep.createStream, undefined);
  validateRedisStreamsEndpoint(ep, false);
  input.dataConnectors.redis.servers[0] = "redis://changed:6379";
  assert.equal(dc.servers[0], "redis://localhost:6379");
});
for (const [name, patch] of Object.entries({
  empty: { servers: [] },
  cluster: { servers: ["redis://a:6379", "redis://b:6379"] },
  sentinel: { servers: ["sentinel://localhost:26379"] },
  protocol: { servers: ["http://localhost:6379"] },
  port: { servers: ["redis://localhost:65536"] },
  emptyPort: { servers: ["redis://localhost:"] },
  portZero: { servers: ["redis://localhost:0"] },
  inlineAuth: { servers: ["redis://user:SECRET@localhost:6379"] },
  emptyAuth: { servers: ["redis://@localhost:6379"] },
  dbPath: { servers: ["redis://localhost:6379/1"] },
  slash: { servers: ["redis://localhost:6379/"] },
  query: { servers: ["redis://localhost:6379?"] },
  fragment: { servers: ["redis://localhost:6379#"] },
  space: { servers: ["redis://localhost:6379 "] },
  database: { redisDatabase: -1 },
  fraction: { redisDatabase: 1.2 },
  timeout: { connectTimeoutMs: 0 },
  overflow: { operationTimeoutMs: 2147483648 },
  token: { token: "SECRET" },
  credentials: { credentialsFile: "secretfile" },
  username: { username: "user" },
  tls: { tlsCaFile: "/ca" },
  keypair: { tlsEnabled: true, tlsCertFile: "/cert" },
  implementation: { implementation: "ioredis" }
}))
  await test(`Redis Streams rejects ${name}`, () => {
    assert.throws(
      () => requireRedisStreamsDataConnectorConfig({ ...base, ...patch }),
      (e) => e instanceof Error && !e.message.includes("SECRET")
    );
  });
const ep: RedisStreamsEndpointConfig = {
  id: 2,
  name: "input",
  idDataConnector: 1,
  properties: {},
  enabled: true,
  streamKey: "key",
  consumerGroup: "group"
};
for (const patch of [
  { streamKey: "" },
  { consumerGroup: "" },
  { readCount: 0 },
  { readCount: 4097 },
  { readBlockMs: 60001 },
  { claimIdleMs: 0 },
  { claimIntervalMs: 0 },
  { publisherMaxInFlight: 1 },
  { createStream: false },
  { groupStartId: "0-0" },
  { createConsumerGroup: true, groupStartId: "invalid" },
  { createConsumerGroup: true, groupStartId: "1-0" },
  { consumerName: "" }
])
  await test(`Redis Streams rejects source ${JSON.stringify(patch)}`, () => {
    assert.throws(() => {
      validateRedisStreamsEndpoint({ ...ep, ...patch }, false);
    });
  });
await test("Redis Streams sink forbids source fields and preserves exact key semantics", () => {
  assert.throws(() => {
    validateRedisStreamsEndpoint(ep, true);
  });
  validateRedisStreamsEndpoint(
    { ...ep, consumerGroup: undefined, createStream: false, publisherMaxInFlight: 4096 },
    true
  );
  requireRedisStreamsDataConnectorConfig({ ...base, password: "runtime-only" });
  requireRedisStreamsDataConnectorConfig({
    ...base,
    servers: ["rediss://localhost:6379"],
    tlsServerName: "verified-peer",
    tlsCaFile: "/ca"
  });
});

await test("Redis Streams accepts omitted port and the two group creation positions", () => {
  requireRedisStreamsDataConnectorConfig({ ...base, servers: ["redis://localhost"] });
  for (const groupStartId of ["0-0", "$"])
    validateRedisStreamsEndpoint({ ...ep, createConsumerGroup: true, groupStartId }, false);
});
