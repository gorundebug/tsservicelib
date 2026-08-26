import assert from "node:assert/strict";
import { test } from "node:test";

import { TemporalWorkflowEnvironment } from "@gorundebug/tsservicelib/datasource/temporal/workflow";
import {
  ConsumedStream,
  MessageContext,
  ServiceStream,
  int32SerdeType,
  makeDefaultSerdeRegistry,
  noopMetrics,
  noopLogger,
  type CanonicalConfig,
  type Completion,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

class RecordingConsumer extends ServiceStream implements TypedStreamConsumer<number> {
  public readonly values: number[] = [];

  public consume(_context: MessageContext, value: number): Completion {
    this.values.push(value);
  }
}

await test("Workflow environment executes the ordinary graph through configured TaskPool", async () => {
  const sourceConfig = stream(1, "source", "Input");
  const targetConfig = stream(2, "target", "Sink", 1);
  const config: CanonicalConfig = {
    services: [
      {
        id: 1,
        name: "workflow-service",
        color: "#000000",
        environment: "test",
        grpcHost: "",
        grpcPort: 0,
        httpHost: "",
        httpPort: 0,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment",
        defaultCallSemantics: { taskPool: { poolName: "workflow-pool" } },
        properties: {}
      }
    ],
    streams: [sourceConfig, targetConfig],
    dataConnectors: [],
    endpoints: [],
    pools: [{ name: "workflow-pool", executorsCount: 2, queueCapacity: 8, properties: {} }],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
  const metrics = new TestMetrics();
  const tracing = new TestTracing();
  const environment = new TemporalWorkflowEnvironment(config, 1, makeDefaultSerdeRegistry(), {
    logger: noopLogger,
    metrics,
    tracing
  });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  const target = new RecordingConsumer(targetConfig, environment);
  environment.registerStream(source);
  environment.registerStream(target);
  source.setConsumer(target);

  await environment.start();
  await source.emit(new MessageContext().withSampling(true), 42);
  await environment.finish();

  assert.deepEqual(target.values, [42]);
  assert.equal(environment.linkCallCount(1, 2), 1);
  assert.equal(
    metrics.counterValue("stream_messages_total", {
      service: "workflow-service",
      from: "source",
      to: "target"
    }),
    1
  );
  assert.deepEqual(
    tracing.spans().map(({ name }) => name),
    ["stream.call"]
  );
  const labels = { service: "workflow-service", name: "workflow-pool" };
  assert.equal(metrics.counterValue("task_pool_tasks_total", labels), 1);
  assert.equal(metrics.gaugeValue("task_pool_executors_allocated", labels), 0);
  assert.equal(metrics.gaugeValue("task_pool_executors_busy", labels), 0);
  assert.equal(metrics.gaugeValue("task_pool_queue_length", labels), 0);
  assert.equal(
    metrics.histogramValue("task_pool_task_execution_duration_seconds", labels)?.count,
    1
  );
});

await test("Workflow PriorityTaskPool uses stable priority then FIFO order", async () => {
  const sourceConfig = stream(1, "source", "Input");
  const targetConfig = stream(2, "target", "Sink", 1);
  const config: CanonicalConfig = {
    services: [
      {
        id: 1,
        name: "workflow-service",
        color: "#000000",
        environment: "test",
        grpcHost: "",
        grpcPort: 0,
        httpHost: "",
        httpPort: 0,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment",
        defaultCallSemantics: {
          priorityTaskPool: { poolName: "workflow-priority", priority: 10 }
        },
        properties: {}
      }
    ],
    streams: [sourceConfig, targetConfig],
    dataConnectors: [],
    endpoints: [],
    pools: [{ name: "workflow-priority", executorsCount: 1, queueCapacity: 8, properties: {} }],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
  const environment = new TemporalWorkflowEnvironment(config, 1, makeDefaultSerdeRegistry(), {
    logger: noopLogger,
    metrics: noopMetrics
  });
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  const target = new RecordingConsumer(targetConfig, environment);
  environment.registerStream(source);
  environment.registerStream(target);
  source.setConsumer(target);

  await source.emit(new MessageContext().withPriority(5), 5);
  await source.emit(new MessageContext().withPriority(1), 1);
  await source.emit(new MessageContext().withPriority(1), 2);
  await environment.start();
  await environment.finish();

  assert.deepEqual(target.values, [1, 2, 5]);
});

function stream<T extends StreamConfig["type"]>(
  id: number,
  name: string,
  type: T,
  idSource = 0
): StreamConfig & { readonly type: T } {
  return {
    id,
    name,
    type,
    pipeline: "workflow",
    idService: 1,
    idSource,
    idSources: [],
    xPos: id,
    yPos: 0,
    properties: {}
  };
}
