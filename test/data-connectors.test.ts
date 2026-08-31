import assert from "node:assert/strict";
import { test } from "node:test";

import { InputStream, SinkStream, SinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  type CanonicalConfig,
  ConsumedStream,
  DataSinkEndpoint,
  DataSinkEndpointConsumer,
  DataSinkEndpointConsumerWithResult,
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  InputDataSource,
  MessageContext,
  OutputDataSink,
  PrometheusMetrics,
  PrometheusMetricsEngine,
  RuntimeConfig,
  RuntimeConfigStore,
  ServiceEnvironment,
  ServiceStream,
  stringSerdeType,
  type InputStreamConfig,
  type SinkStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";

const inputConfig: InputStreamConfig = {
  id: 1,
  name: "input",
  properties: {},
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0,
  valueType: "string",
  idEndpoint: 100
};

const sourceConfig: StreamConfig = {
  id: 2,
  name: "source",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0
};

const sinkConfig: SinkStreamConfig = {
  id: 3,
  name: "sink",
  properties: {},
  type: "Sink",
  pipeline: "main",
  idService: 1,
  idSource: 2,
  idSources: [],
  xPos: 2,
  yPos: 0,
  idEndpoint: 200,
  valueType: "string"
};

const inputConsumerConfig: StreamConfig = {
  id: 4,
  name: "inputConsumer",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 1
};

function config(sourceImplementation = "http"): CanonicalConfig {
  return {
    services: [
      {
        id: 1,
        name: "service",
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
    streams: [inputConfig, sourceConfig, sinkConfig, inputConsumerConfig],
    dataConnectors: [
      {
        id: 10,
        name: "sourceConnector",
        type: 4,
        implementation: sourceImplementation,
        properties: { address: "first" }
      },
      {
        id: 20,
        name: "sinkConnector",
        type: 4,
        implementation: "http",
        properties: {}
      }
    ],
    endpoints: [
      {
        id: 100,
        name: "sourceEndpoint",
        idDataConnector: 10,
        properties: {}
      },
      {
        id: 200,
        name: "sinkEndpoint",
        idDataConnector: 20,
        properties: {}
      }
    ],
    pools: [],
    links: [],
    modules: [],
    types: [],
    properties: {}
  };
}

class TestDataSource extends InputDataSource {
  public constructor(id: number, environment: ServiceEnvironment) {
    super(id, environment);
  }

  public start(): Promise<void> {
    return Promise.resolve();
  }

  public stop(): Promise<void> {
    return Promise.resolve();
  }
}

class TestDataSink extends OutputDataSink {
  public constructor(id: number, environment: ServiceEnvironment) {
    super(id, environment);
  }

  public start(): Promise<void> {
    return Promise.resolve();
  }

  public stop(): Promise<void> {
    return Promise.resolve();
  }
}

class RecordingStream extends ServiceStream implements TypedStreamConsumer<string> {
  public readonly values: string[] = [];

  public consume(context: MessageContext, value: string): void {
    void context;
    this.values.push(value);
  }
}

await test("connector and endpoint identities are stable while reloadable config stays live", async () => {
  const store = new RuntimeConfigStore(new RuntimeConfig(config()));
  const environment = new ServiceEnvironment(store, 1);
  const source = new TestDataSource(10, environment);
  const sink = new TestDataSink(20, environment);
  const sourceEndpoint = new DataSourceEndpoint(source, 100);
  const sinkEndpoint = new DataSinkEndpoint(sink, 200);

  source.addEndpoint(sourceEndpoint);
  sink.addEndpoint(sinkEndpoint);
  environment.addDataSource(source);
  environment.addDataSink(sink);

  assert.equal(source.name, "sourceConnector");
  assert.equal(sourceEndpoint.name, "sourceEndpoint");
  assert.equal(source.endpoint(100), sourceEndpoint);
  assert.equal(sink.endpoint(200), sinkEndpoint);
  assert.equal(environment.dataSourceById(10), source);
  assert.equal(environment.dataSinkById(20), sink);
  assert.deepEqual(environment.dataSources(), [source]);
  assert.deepEqual(environment.dataSinks(), [sink]);

  await store.reload(() => Promise.resolve(new RuntimeConfig(config("fetch"))));

  assert.equal(source.name, "sourceConnector");
  assert.equal(sourceEndpoint.name, "sourceEndpoint");
  assert.equal(source.config().implementation, "fetch");
});

await test("endpoints reject ownership by the wrong connector", () => {
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(config())),
    1
  );
  const source = new TestDataSource(10, environment);
  const sink = new TestDataSink(20, environment);

  assert.throws(() => new DataSourceEndpoint(source, 200), /belongs to connector 20, not 10/);
  assert.throws(() => new DataSinkEndpoint(sink, 100), /belongs to connector 10, not 20/);
});

await test("endpoint consumers retain the canonical typed stream boundaries", async () => {
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(config())),
    1
  );
  const source = new TestDataSource(10, environment);
  const sink = new TestDataSink(20, environment);
  const sourceEndpoint = new DataSourceEndpoint(source, 100);
  const sinkEndpoint = new DataSinkEndpoint(sink, 200);
  const serde = environment.serde(stringSerdeType);
  const input = new InputStream<string, string, string>(inputConfig, environment, serde, serde);
  const inputDownstream = new RecordingStream(inputConsumerConfig, environment);
  const sinkSource = new ConsumedStream(sourceConfig, environment, serde);
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, stringSerdeType);
  const sinkStream = new SinkStream(sinkConfig, sinkSource);
  const resultSource = new ConsumedStream(
    { ...sourceConfig, id: 5, name: "resultSource" },
    environment,
    serde
  );
  const sinkWithResultConfig = { ...sinkConfig, id: 6, name: "sinkWithResult" };
  environment.serdeRegistry().registerStreamErrorType(sinkWithResultConfig.id, stringSerdeType);
  const sinkWithResult = new SinkStreamWithResult(sinkWithResultConfig, resultSource);
  const sourceConsumer = new DataSourceEndpointConsumer(sourceEndpoint, input);
  const sinkConsumer = new DataSinkEndpointConsumer(sinkEndpoint, sinkStream);
  const sinkResultConsumer = new DataSinkEndpointConsumerWithResult(sinkEndpoint, sinkWithResult);

  input.setConsumer(inputDownstream);
  sourceEndpoint.addEndpointConsumer(sourceConsumer);
  sinkEndpoint.addEndpointConsumer(sinkConsumer);
  sinkEndpoint.addEndpointConsumer(sinkResultConsumer);
  await sourceConsumer.consume(new MessageContext(), "value");

  assert.deepEqual(inputDownstream.values, ["value"]);
  assert.equal(sourceConsumer.stream(), input);
  assert.equal(sinkConsumer.stream(), sinkStream);
  assert.equal(sinkResultConsumer.stream(), sinkWithResult);
  assert.deepEqual(sourceEndpoint.endpointConsumers(), [sourceConsumer]);
  assert.deepEqual(sinkEndpoint.endpointConsumers(), [sinkConsumer, sinkResultConsumer]);
});

await test("gRPC endpoint metrics carry the canonical protocol label", () => {
  const base = config();
  const grpcConfig: CanonicalConfig = {
    ...base,
    dataConnectors: base.dataConnectors.map((connector) => ({
      ...connector,
      type: 2,
      connectionsCount: 1
    })),
    endpoints: base.endpoints.map((endpoint) => ({
      ...endpoint,
      grpcMethodType: "NoStreaming" as const,
      methodName: endpoint.name
    }))
  };
  const metrics = new TestMetrics();
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(grpcConfig)),
    1,
    undefined,
    undefined,
    undefined,
    undefined,
    metrics
  );
  new DataSourceEndpoint(new TestDataSource(10, environment), 100);
  new DataSinkEndpoint(new TestDataSink(20, environment), 200);

  assert.equal(
    metrics.gaugeValue("datasource_endpoint_active_requests", {
      connector: "sourceConnector",
      endpoint: "sourceEndpoint",
      protocol: "grpc"
    }),
    0
  );
  assert.equal(
    metrics.gaugeValue("datasink_endpoint_active_requests", {
      connector: "sinkConnector",
      endpoint: "sinkEndpoint",
      protocol: "grpc"
    }),
    0
  );
});

await test("data source lifecycle gauges exist before the first request", async () => {
  const metrics = new PrometheusMetrics();
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(config())),
    1,
    undefined,
    undefined,
    undefined,
    undefined,
    metrics
  );
  new DataSourceEndpoint(new TestDataSource(10, environment), 100);

  const rendered = await new PrometheusMetricsEngine(metrics).render();
  assert.match(
    rendered,
    /datasource_endpoint_active_requests\{connector="sourceConnector",endpoint="sourceEndpoint",protocol=""\} 0/u
  );
  assert.match(
    rendered,
    /datasource_endpoint_pending_requests\{connector="sourceConnector",endpoint="sourceEndpoint",protocol=""\} 0/u
  );
});
