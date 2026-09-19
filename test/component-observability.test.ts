import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ConsumedStream,
  MessageContext,
  int32SerdeType,
  parseCanonicalConfig,
  type MapStreamConfig,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeMapStream } from "@gorundebug/tsservicelib/operators";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";
import { makeTestEnvironment } from "./support/environment.js";

await test("link metrics and spans use receiving pipeline and component definition", async () => {
  const sourceConfig: StreamConfig = {
    id: 1,
    name: "Input",
    type: "Input",
    pipeline: "entry",
    component: "Request",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: 0,
    yPos: 0,
    properties: {}
  };
  const targetConfig: MapStreamConfig = {
    ...sourceConfig,
    id: 2,
    name: "Calculate Price",
    type: "Map",
    idSource: 1,
    pipeline: "pricing",
    component: "Customer Pricing",
    valueType: "int32"
  };
  const metrics = new TestMetrics(),
    tracing = new TestTracing();
  const environment = makeTestEnvironment([sourceConfig, targetConfig], { metrics, tracing });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  makeMapStream(targetConfig, source, {
    map(context, _stream, value, out) {
      return out.out(context, value);
    }
  });
  await source.emit(new MessageContext(), 1);
  assert.deepEqual(tracing.spans(), []);
  await source.emit(new MessageContext().withSampling(true), 2);
  assert.equal(
    metrics.counterValue("stream_messages_total", {
      service: "test-service",
      from: "Input",
      to: "Calculate Price",
      pipeline: "pricing",
      component: "Customer Pricing"
    }),
    2
  );
  assert.equal(environment.linkCallCount(1, 2), 2);
  const spans = tracing.spans();
  assert.equal(spans.length, 2, "No component wrapper spans are created");
  for (const span of spans) {
    const attributes = Object.fromEntries(span.attributes.map(({ key, value }) => [key, value]));
    assert.equal(attributes["pipeline"], "pricing");
    assert.equal(attributes["component"], "Customer Pricing");
    assert.equal(attributes["component_instance"], undefined);
  }
});

await test("config parser preserves optional component without hiding it in properties", () => {
  const stream = {
    id: 1,
    name: "Calculate Price",
    type: "Map",
    pipeline: "pricing",
    component: "Customer Pricing",
    valueType: "int32",
    idService: 1,
    idSource: 0,
    xPos: 0,
    yPos: 0
  };
  const config = parseCanonicalConfig({
    services: {},
    streams: { price: stream },
    dataConnectors: {},
    endpoints: {},
    pools: {},
    links: {},
    modules: {},
    types: {}
  });
  assert.equal(config.streams[0]?.component, "Customer Pricing");
  assert.equal(config.streams[0].properties["component"], undefined);
  assert.throws(
    () => parseCanonicalConfig({ streams: { price: { ...stream, component: 42 } } }),
    /component.*string/
  );
});

await test("observability does not require auxiliary feedback nodes in canonical config", async () => {
  const sourceConfig: StreamConfig = {
    id: 1,
    name: "Input",
    type: "Input",
    pipeline: "entry",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: 0,
    yPos: 0,
    properties: {}
  };
  const metrics = new TestMetrics(),
    tracing = new TestTracing();
  const environment = makeTestEnvironment([sourceConfig], { metrics, tracing });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  const config: StreamConfig = { ...sourceConfig, id: 99, name: "Feedback" };
  let consumed = 0;
  const consumer = {
    id: 99,
    name: "Feedback",
    transformationName: "map",
    runtimeEnvironment: () => environment,
    config: () => {
      throw new Error("not a canonical graph node");
    },
    consume: (_context: MessageContext, value: number) => {
      consumed += value;
    }
  };
  // The Stream interface permits adapters whose config accessor is unavailable;
  // collecting labels must not introduce a new call to that accessor.
  void config;
  await environment
    .makeCaller(source, consumer)
    .consume(new MessageContext().withSampling(true), 3);
  assert.equal(consumed, 3);
  assert.equal(
    metrics.counterValue("stream_messages_total", {
      service: "test-service",
      from: "Input",
      to: "Feedback",
      pipeline: "",
      component: ""
    }),
    1
  );
});
