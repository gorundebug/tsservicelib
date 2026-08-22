import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Context,
  ConsumedStream,
  int32SerdeType,
  MessageContext,
  PriorityTaskPool,
  RuntimeCallerFactory,
  RuntimeConfig,
  RuntimeConfigStore,
  RuntimeTaskRegistry,
  ServiceEnvironment,
  SpanStatusCode,
  TaskPool,
  boolAttribute,
  noopTracing,
  spanError,
  stringAttribute,
  type CanonicalConfig,
  type MapStreamConfig,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeMapStream, type MapFunction } from "@gorundebug/tsservicelib/operators";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";
import { makeTestEnvironment } from "./support/environment.js";

await test("runtime removes noop tracing before the request path", () => {
  let enabledCalls = 0;
  let tracerCalls = 0;
  const disabledTracing = {
    enabled(): boolean {
      enabledCalls += 1;
      return false;
    },
    tracer(): never {
      tracerCalls += 1;
      throw new Error("disabled tracing must not resolve a tracer");
    }
  };
  const environment = makeTestEnvironment([], { tracing: disabledTracing });
  assert.equal(environment.tracing(), undefined);
  assert.equal(enabledCalls, 1);
  assert.equal(tracerCalls, 0);

  // The exported no-op backend follows the same construction-only branch.
  assert.equal(makeTestEnvironment([], { tracing: noopTracing }).tracing(), undefined);
});

await test("test tracing records attributes, events, errors and exactly one end", () => {
  const tracing = new TestTracing();
  const started = tracing
    .tracer("orderservice")
    .start(new MessageContext(), "http.input", [stringAttribute("stream", "orders")]);
  started.span.setAttributes([boolAttribute("has_result", true)]);
  started.span.addEvent("consume_message", [stringAttribute("message_id", "order-1")]);
  const failure = new Error("handler failed");
  spanError(started.span, failure);
  started.span.end();
  started.span.end();

  const spans = tracing.spans();
  assert.equal(spans.length, 1);
  const span = spans[0];
  assert.ok(span);
  assert.equal(span.tracerName, "orderservice");
  assert.equal(span.name, "http.input");
  assert.equal(span.statusCode, SpanStatusCode.Error);
  assert.equal(span.statusDescription, "handler failed");
  assert.equal(span.error, failure);
  assert.deepEqual(
    span.attributes.map(({ key, value }) => [key, value]),
    [
      ["stream", "orders"],
      ["has_result", true]
    ]
  );
  assert.deepEqual(
    span.events.map(({ name }) => name),
    ["consume_message"]
  );
});

await test("sampled stream delivery records Go-compatible call and operator spans", async () => {
  const sourceConfig: StreamConfig = {
    id: 1,
    name: "Input",
    properties: {},
    type: "Input",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: 0,
    yPos: 0
  };
  const mapConfig: MapStreamConfig = {
    ...sourceConfig,
    id: 2,
    name: "Map Order",
    type: "Map",
    idSource: 1,
    valueType: "int32"
  };
  const tracing = new TestTracing();
  const environment = makeTestEnvironment([sourceConfig, mapConfig], { tracing });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  const function_: MapFunction<number, number> = {
    map(context, _stream, value, out) {
      return out.out(context, value);
    }
  };
  makeMapStream(mapConfig, source, function_);

  await source.emit(new MessageContext(), 1);
  assert.deepEqual(tracing.spans(), []);

  await source.emit(new MessageContext().withSampling(true), 2);
  assert.deepEqual(
    tracing.spans().map(({ name, attributes }) => ({
      name,
      attributes: Object.fromEntries(attributes.map(({ key, value }) => [key, value]))
    })),
    [
      { name: "stream.map", attributes: { stream: "Map Order" } },
      { name: "stream.call", attributes: { from: "Input", to: "Map Order" } }
    ]
  );
});

await test("sampled stream links record their resolved pooled call semantics", async () => {
  const stream = (id: number, name: string, idSource = 0): StreamConfig => ({
    id,
    name,
    properties: {},
    type: idSource === 0 ? "Input" : "Map",
    pipeline: "main",
    idService: 1,
    idSource,
    idSources: idSource === 0 ? [] : [idSource],
    xPos: 0,
    yPos: 0
  });
  const streams = [
    stream(1, "Task Input"),
    { ...stream(2, "Task Map", 1), valueType: "int32" } as MapStreamConfig,
    stream(3, "Priority Input"),
    { ...stream(4, "Priority Map", 3), valueType: "int32" } as MapStreamConfig,
    stream(5, "Parallel Input"),
    { ...stream(6, "Parallel Map", 5), valueType: "int32" } as MapStreamConfig
  ];
  const config: CanonicalConfig = {
    services: [
      {
        id: 1,
        name: "test-service",
        color: "#000000",
        properties: {},
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
        kubernetesWorkloadType: "Deployment"
      }
    ],
    streams,
    dataConnectors: [],
    endpoints: [],
    pools: [
      { name: "Tasks", executorsCount: 1, queueCapacity: 0, properties: {} },
      { name: "Priorities", executorsCount: 1, queueCapacity: 0, properties: {} }
    ],
    links: [
      { from: 1, to: 2, callSemantics: { taskPool: { poolName: "Tasks" } }, properties: {} },
      {
        from: 3,
        to: 4,
        callSemantics: { priorityTaskPool: { poolName: "Priorities", priority: 0 } },
        properties: {}
      },
      { from: 5, to: 6, callSemantics: { parallelCall: {} }, properties: {} }
    ],
    modules: [],
    types: [],
    properties: {}
  };
  const store = new RuntimeConfigStore(new RuntimeConfig(config));
  const taskPool = new TaskPool({ name: "Tasks", executorsCount: 1 });
  const priorityPool = new PriorityTaskPool({ name: "Priorities", executorsCount: 1 });
  const runtimeTasks = new RuntimeTaskRegistry();
  const taskPools = new Map([[taskPool.name(), taskPool]]);
  const priorityPools = new Map([[priorityPool.name(), priorityPool]]);
  const tracing = new TestTracing();
  const callerFactory = new RuntimeCallerFactory({
    config: () => store.current(),
    serviceId: 1,
    taskPools,
    priorityTaskPools: priorityPools,
    tasks: runtimeTasks
  });
  const environment = new ServiceEnvironment(
    store,
    1,
    callerFactory,
    undefined,
    undefined,
    undefined,
    undefined,
    tracing,
    taskPools,
    priorityPools
  );
  const function_: MapFunction<number, number> = {
    map(context, _stream, value, out) {
      return out.out(context, value);
    }
  };
  const [
    taskInputConfig,
    taskMapConfig,
    priorityInputConfig,
    priorityMapConfig,
    parallelInputConfig,
    parallelMapConfig
  ] = streams;
  assert.ok(taskInputConfig);
  assert.ok(taskMapConfig);
  assert.ok(priorityInputConfig);
  assert.ok(priorityMapConfig);
  assert.ok(parallelInputConfig);
  assert.ok(parallelMapConfig);
  const taskInput = new ConsumedStream(
    taskInputConfig,
    environment,
    environment.serde(int32SerdeType)
  );
  const priorityInput = new ConsumedStream(
    priorityInputConfig,
    environment,
    environment.serde(int32SerdeType)
  );
  const parallelInput = new ConsumedStream(
    parallelInputConfig,
    environment,
    environment.serde(int32SerdeType)
  );
  makeMapStream(taskMapConfig as MapStreamConfig, taskInput, function_);
  makeMapStream(priorityMapConfig as MapStreamConfig, priorityInput, function_);
  makeMapStream(parallelMapConfig as MapStreamConfig, parallelInput, function_);

  const context = new MessageContext().withSampling(true);
  await taskPool.start(Context.background());
  await priorityPool.start(Context.background());
  await taskInput.emit(context, 1);
  await priorityInput.emit(context, 2);
  await parallelInput.emit(context, 3);
  await taskPool.stop(Context.background());
  await priorityPool.stop(Context.background());
  runtimeTasks.stopAdmission();
  await runtimeTasks.drain();

  assert.deepEqual(
    tracing
      .spans()
      .filter(({ name }) => name === "stream.call")
      .map(({ attributes }) =>
        Object.fromEntries(attributes.map(({ key, value }) => [key, value]))
      ),
    [
      { from: "Task Input", to: "Task Map", type: "taskpool", taskpoolname: "Tasks" },
      {
        from: "Priority Input",
        to: "Priority Map",
        type: "prioritytaskpool",
        taskpoolname: "Priorities"
      },
      { from: "Parallel Input", to: "Parallel Map", type: "parallel" }
    ]
  );
});
