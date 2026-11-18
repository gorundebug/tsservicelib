// Deterministic adapter tests: native sessions are mocked, graph/endpoint/metrics are real.
import assert from "node:assert/strict";
import { test } from "node:test";
import { makeTestEnvironment } from "../../dist-test/test/support/environment.js";
import {
  Context,
  MessageContext,
  ConsumedStream,
  stringSerdeType,
  errorSerdeType
} from "../../dist/runtime/index.js";
import { makeSinkStream } from "../../dist/operators/index.js";
import { TestMetrics } from "../../dist/runtime/testmetrics/index.js";

const cases = [
  [
    "pulsar",
    11,
    "pulsar://localhost",
    "pulsar-client-node",
    "Pulsar",
    { topic: "persistent://public/default/events" }
  ],
  [
    "redisstreams",
    10,
    "redis://localhost",
    "node-redis",
    "RedisStreams",
    { streamKey: "events", createStream: false }
  ],
  [
    "rabbitmq",
    9,
    "amqp://localhost",
    "rabbitmq-amqp-js-client",
    "RabbitMQ",
    { queueName: "events", createQueue: false, createExchange: false, createBinding: false }
  ],
  [
    "nats",
    7,
    "nats://localhost",
    "@nats-io/transport-node",
    "NATS",
    { subject: "events", createStream: false, createConsumer: false }
  ],
  [
    "jetstream",
    8,
    "nats://localhost",
    "@nats-io/transport-node",
    "NATS",
    { subject: "events", streamName: "events", createStream: false, createConsumer: false }
  ]
];
for (const [protocol, type, server, implementation, prefix, options] of cases) {
  await test(`${protocol}: publication outcome boundaries`, async (t) => {
    const folder = protocol === "jetstream" ? "nats" : protocol;
    const adapter = await import(`../../dist/datasink/${folder}/index.js`);
    const metrics = new TestMetrics();
    const upstreamConfig = {
      id: 1,
      name: "input",
      type: "Map",
      pipeline: "main",
      properties: {},
      idService: 1,
      idSource: 0,
      idSources: [],
      xPos: 0,
      yPos: 0,
      valueType: "string"
    };
    const sinkConfig = {
      ...upstreamConfig,
      id: 2,
      name: "output",
      type: "Sink",
      idSource: 1,
      idEndpoint: 101
    };
    const env = makeTestEnvironment([upstreamConfig, sinkConfig], {
      metrics,
      dataConnectors: [
        {
          id: 10,
          name: "events",
          type,
          implementation,
          properties: {},
          servers: [server],
          redisDatabase: 0,
          adminUrl: "http://localhost",
          connectTimeoutMs: 1000,
          operationTimeoutMs: 1000
        }
      ],
      endpoints: [
        { id: 101, name: "publish", idDataConnector: 10, properties: {}, enabled: true, ...options }
      ]
    });
    env.serdeRegistry().registerStreamErrorType(2, errorSerdeType);
    const upstream = new ConsumedStream(upstreamConfig, env, env.serde(stringSerdeType));
    const output = makeSinkStream(sinkConfig, upstream);
    adapter[`make${prefix}EndpointConsumer`](output, {
      getStreamId: () => "test",
      beginRequest: (c) => ({ context: c, state: undefined }),
      consumeMessage: () => {},
      endRequest: () => {}
    });
    const sink = env.dataSinkById(10);
    const endpoint = sink.endpoint(101);
    const count = (outcome) =>
      metrics.counterValue("broker_endpoint_publication_outcomes_total", {
        protocol,
        connector: "events",
        endpoint: "publish",
        outcome
      });
    const message = new adapter.SinkMessage(endpoint, () => {});
    const refusal = folder === "nats" ? "error" : "not-sent";
    assert.throws(() => message.send(new MessageContext(), () => {}));
    assert.equal(count(refusal), 1);
    message.retire();
    await assert.rejects(message.sendSync(new MessageContext()));
    assert.equal(count(refusal), 2);
    if (folder === "nats") return; // real native NATS outcome tests remain separate
    const native = await import(
      `../../dist/internal/${folder === "redisstreams" ? "redis" : folder}/client.js`
    );
    t.mock.method(native.Session.prototype, "start", async function () {
      this.ready = true;
    });
    t.mock.method(native.Session.prototype, "close", async function () {
      this.ready = false;
    });
    let complete;
    t.mock.method(
      native.Session.prototype,
      "publish",
      () =>
        new Promise((resolve) => {
          complete = resolve;
        })
    );
    await sink.start(new Context());
    const asyncMessage = new adapter.SinkMessage(endpoint, () => {});
    if (folder === "redisstreams") asyncMessage.fields = [["value", "event"]];
    let observed;
    asyncMessage.send(new MessageContext(), (delivery) => {
      assert.equal(count("accepted"), 1);
      observed = delivery;
    });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(count("accepted"), 0);
    const outcome = { outcome: folder === "pulsar" ? "Accepted" : "accepted" };
    complete(outcome);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(observed, outcome);
    assert.equal(count("accepted"), 1);
    const syncMessage = new adapter.SinkMessage(endpoint, () => assert.fail("sync must not emit"));
    if (folder === "redisstreams") syncMessage.fields = [["value", "event"]];
    const pending = syncMessage.sendSync(new MessageContext());
    await new Promise((resolve) => setImmediate(resolve));
    complete(outcome);
    assert.equal(await pending, outcome);
    assert.equal(count("accepted"), 2);
    await sink.stop(new Context());
  });
}

await test("publication outcomes render through the production Prometheus exporter", async () => {
  const { PublicationMetrics } = await import("../../dist/internal/broker-publication-metrics.js");
  const { PrometheusMetrics, PrometheusMetricsEngine } =
    await import("../../dist/runtime/index.js");
  const metrics = new PrometheusMetrics();
  const profiles = {
    nats: ["client-accepted", "flushed", "error"],
    jetstream: ["acknowledged", "error"],
    pulsar: ["accepted", "not-sent", "rejected", "unknown"],
    redisstreams: ["accepted", "not-sent", "rejected", "unknown"],
    rabbitmq: ["accepted", "not-sent", "rejected", "unknown", "unroutable", "modified"]
  };
  const context = Context.background();
  for (const [protocol, outcomes] of Object.entries(profiles)) {
    const publication = new PublicationMetrics(metrics, protocol, "events", "publish");
    for (const outcome of outcomes) publication.observe(context, outcome);
  }
  const text = await new PrometheusMetricsEngine(metrics).render();
  const lines = text
    .split("\n")
    .filter(
      (line) =>
        line.startsWith("broker_endpoint_publication_outcomes_total{") && line.endsWith(" 1")
    );
  assert.equal(lines.length, 19);
  for (const [protocol, outcomes] of Object.entries(profiles)) {
    for (const outcome of outcomes) {
      assert.equal(
        lines.filter(
          (line) =>
            line.includes(`protocol="${protocol}"`) &&
            line.includes(`outcome="${outcome}"`) &&
            line.includes('connector="events"') &&
            line.includes('endpoint="publish"')
        ).length,
        1
      );
    }
  }
  assert.ok(!text.includes("publication_outcomes_total_total"));
});
