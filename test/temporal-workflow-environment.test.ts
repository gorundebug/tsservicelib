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

class BlockingConsumer extends ServiceStream implements TypedStreamConsumer<number> {
  public active = 0;
  public maximumActive = 0;
  public completed = 0;
  readonly #release: Promise<void>;
  readonly #entered: () => void;

  public constructor(
    config: StreamConfig,
    environment: TemporalWorkflowEnvironment,
    release: Promise<void>,
    entered: () => void
  ) {
    super(config, environment);
    this.#release = release;
    this.#entered = entered;
  }

  public async consume(): Promise<void> {
    this.active += 1;
    this.maximumActive = Math.max(this.maximumActive, this.active);
    if (this.active === 2) this.#entered();
    await this.#release;
    this.completed += 1;
    this.active -= 1;
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

await test("Workflow TaskPool limits logical executors and finish drains", async () => {
  const config = workflowPoolConfig(2);
  const environment = new TemporalWorkflowEnvironment(config, 1, makeDefaultSerdeRegistry(), {
    logger: noopLogger,
    metrics: noopMetrics
  });
  const [sourceConfig, targetConfig] = config.streams;
  if (sourceConfig === undefined || targetConfig === undefined)
    throw new Error("workflow pool fixture streams are missing");
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  let releaseTasks!: () => void;
  const release = new Promise<void>((resolve) => {
    releaseTasks = resolve;
  });
  let reportTwoEntered!: () => void;
  const twoEntered = new Promise<void>((resolve) => {
    reportTwoEntered = resolve;
  });
  const target = new BlockingConsumer(targetConfig, environment, release, reportTwoEntered);
  environment.registerStream(source);
  environment.registerStream(target);
  source.setConsumer(target);

  await environment.start();
  for (let value = 0; value < 5; value += 1) await source.emit(new MessageContext(), value);
  await twoEntered;
  assert.equal(target.maximumActive, 2);
  let completed = false;
  const completion = environment.waitForCompletion(Promise.resolve(42)).then((value) => {
    completed = true;
    return value;
  });
  await Promise.resolve();
  assert.equal(completed, false);
  releaseTasks();
  assert.equal(await completion, 42);
  await environment.finish();

  assert.equal(target.completed, 5);
  assert.equal(target.active, 0);
});

await test("Workflow TaskPool propagates failure and rejects canceled admission", async () => {
  const config = workflowPoolConfig(1);
  const environment = new TemporalWorkflowEnvironment(config, 1, makeDefaultSerdeRegistry(), {
    logger: noopLogger,
    metrics: noopMetrics
  });
  const [sourceConfig, targetConfig] = config.streams;
  if (sourceConfig === undefined || targetConfig === undefined)
    throw new Error("workflow pool fixture streams are missing");
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(int32SerdeType));
  class FailingConsumer extends ServiceStream implements TypedStreamConsumer<number> {
    public consume(): Completion {
      throw new Error("expected workflow pool failure");
    }
  }
  const target = new FailingConsumer(targetConfig, environment);
  environment.registerStream(source);
  environment.registerStream(target);
  source.setConsumer(target);
  await environment.start();
  await source.emit(new MessageContext(), 1);
  await assert.rejects(
    environment.waitForCompletion(new Promise<number>(() => undefined)),
    /expected workflow pool failure/
  );
  await assert.rejects(environment.finish(), /expected workflow pool failure/);

  const canceledEnvironment = new TemporalWorkflowEnvironment(
    config,
    1,
    makeDefaultSerdeRegistry(),
    { logger: noopLogger, metrics: noopMetrics }
  );
  const canceledSource = new ConsumedStream(
    sourceConfig,
    canceledEnvironment,
    canceledEnvironment.serde(int32SerdeType)
  );
  const canceledTarget = new RecordingConsumer(targetConfig, canceledEnvironment);
  canceledEnvironment.registerStream(canceledSource);
  canceledEnvironment.registerStream(canceledTarget);
  canceledSource.setConsumer(canceledTarget);
  await canceledEnvironment.start();
  const controller = new AbortController();
  controller.abort(new Error("expected workflow pool cancellation"));
  await canceledSource.emit(new MessageContext(controller.signal), 2);
  await assert.rejects(canceledEnvironment.finish(), /expected workflow pool cancellation/);
  assert.deepEqual(canceledTarget.values, []);
});

function workflowPoolConfig(executorsCount: number): CanonicalConfig {
  const sourceConfig = stream(1, "source", "Input");
  const targetConfig = stream(2, "target", "Sink", 1);
  return {
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
    pools: [{ name: "workflow-pool", executorsCount, queueCapacity: 8, properties: {} }],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
}

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
