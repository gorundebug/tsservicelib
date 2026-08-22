import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { test } from "node:test";

import {
  type Client,
  type EndpointHandler,
  NodeHttpClient,
  type Request,
  type Requester,
  type Response,
  type StreamContext,
  makeNodeHttpEndpointConsumer
} from "@gorundebug/tsservicelib/datasink/http";
import { SinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  type CanonicalConfig,
  ConsumedStream,
  Context,
  type DataSink,
  type HttpDataConnectorConfig,
  type HttpEndpointConfig,
  MessageContext,
  type Metrics,
  errorSerdeType,
  RuntimeConfig,
  RuntimeConfigStore,
  ServiceEnvironment,
  ServiceStream,
  type SinkStreamConfig,
  type StreamConfig,
  type Tracing,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";
import { makeTestSerde } from "./support/environment.js";

const sourceConfig: StreamConfig = {
  id: 1,
  name: "source",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 0,
  idSources: [],
  xPos: 0,
  yPos: 0
};

const sinkConfig: SinkStreamConfig = {
  id: 2,
  name: "inventoryCall",
  properties: {},
  type: "Sink",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0,
  idEndpoint: 100,
  valueType: "string"
};

const resultConfig: StreamConfig = {
  id: 3,
  name: "result",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 2,
  idSources: [],
  xPos: 2,
  yPos: 0
};

function canonicalConfig(): CanonicalConfig<HttpDataConnectorConfig, HttpEndpointConfig> {
  return {
    services: [
      {
        id: 1,
        name: "orderservice",
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
    streams: [sourceConfig, sinkConfig, resultConfig],
    dataConnectors: [
      {
        id: 10,
        name: "inventoryServiceApi",
        type: 1,
        implementation: "typescript/nodeHttp",
        host: "inventoryservice",
        port: 9092,
        useDedicatedListener: false,
        properties: {}
      }
    ],
    endpoints: [
      {
        id: 100,
        name: "processOrderItem",
        idDataConnector: 10,
        httpMethodType: "POST",
        path: "/v1/processorderitem",
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

class RecordingStream extends ServiceStream implements TypedStreamConsumer<string> {
  public readonly values: string[] = [];

  public consume(context: MessageContext, value: string): void {
    void context;
    this.values.push(value);
  }
}

interface HandlerState {
  readonly kind: "request";
}

type FailureMode = "begin" | "consume" | "missing-request" | "client" | "handle";

class LifecycleHandler implements EndpointHandler<
  HandlerState,
  string,
  string,
  string,
  string,
  Error
> {
  public readonly events: string[] = [];
  readonly #mode: FailureMode;
  readonly #url: string;

  public constructor(mode: FailureMode, url = "http://127.0.0.1/") {
    this.#mode = mode;
    this.#url = url;
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: HandlerState }> {
    this.events.push("begin");
    if (this.#mode === "begin") {
      return Promise.reject(new Error("begin failed"));
    }
    return Promise.resolve({ context, state: { kind: "request" } });
  }

  public consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: HandlerState,
    value: string,
    requester: Requester
  ): void {
    void stream;
    void state;
    this.events.push(`consume:${value}`);
    if (this.#mode === "consume") {
      throw new Error("consume failed");
    }
    if (this.#mode !== "missing-request") {
      requester.newRequest(context, "POST", this.#url, value);
    }
  }

  public handleResponse(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: HandlerState,
    response: Response
  ): void {
    void context;
    void stream;
    void state;
    void response;
    this.events.push("handle");
    if (this.#mode === "handle") {
      throw new Error("handle failed");
    }
  }

  public endRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    error: Error | undefined,
    state: HandlerState
  ): void {
    void context;
    void stream;
    void state;
    this.events.push(error === undefined ? "end:ok" : `end:${error.message}`);
  }
}

class RejectingClient implements Client {
  public calls = 0;
  public closed = false;
  readonly #error: Error;

  public constructor(error: Error) {
    this.#error = error;
  }

  public do(request: Request): Promise<Response> {
    void request;
    this.calls += 1;
    return Promise.reject(this.#error);
  }

  public close(context: Context): Promise<void> {
    void context;
    this.closed = true;
    return Promise.resolve();
  }
}

interface SinkHarness {
  readonly source: ConsumedStream<string>;
  readonly dataSink: DataSink;
  readonly result: RecordingStream;
}

function makeSinkHarness(
  handler: EndpointHandler<HandlerState, string, string, string, string, Error>,
  client: Client,
  metrics?: Metrics,
  tracing?: Tracing
): SinkHarness {
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(canonicalConfig())),
    1,
    undefined,
    undefined,
    undefined,
    undefined,
    metrics,
    tracing
  );
  const source = new ConsumedStream(sourceConfig, environment, makeTestSerde<string>());
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  const sink = new SinkStreamWithResult(sinkConfig, source);
  const result = new RecordingStream(resultConfig, environment);
  sink.setConsumer(result);
  makeNodeHttpEndpointConsumer(sink, client, handler);
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink);
  return { source, dataSink, result };
}

class TestHandler implements EndpointHandler<HandlerState, string, string, string, string, Error> {
  public readonly events: string[] = [];
  readonly #url: string;
  readonly #responseLimit: number;

  public constructor(url: string, responseLimit = 1_024) {
    this.#url = url;
    this.#responseLimit = responseLimit;
  }

  public beginRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>
  ): Promise<{ readonly context: MessageContext; readonly state: HandlerState }> {
    assert.equal(stream.endpointConfig.path, "/v1/processorderitem");
    assert.equal(stream.dataConnectorConfig.host, "inventoryservice");
    this.events.push("begin");
    return Promise.resolve({ context, state: { kind: "request" } });
  }

  public consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: HandlerState,
    value: string,
    requester: Requester
  ): void {
    void stream;
    void state;
    this.events.push(`consume:${value}`);
    const request = requester.newRequest(context, "POST", this.#url, value);
    request.headers.set("content-type", "text/plain");
  }

  public async handleResponse(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: HandlerState,
    response: Response
  ): Promise<void> {
    void state;
    this.events.push(`response:${String(response.statusCode)}`);
    await stream.collect(context, await response.text(this.#responseLimit));
  }

  public endRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    error: Error | undefined,
    state: HandlerState
  ): void {
    void context;
    void stream;
    void state;
    this.events.push(error === undefined ? "end:ok" : `end:${error.message}`);
  }
}

await test("Node HTTP sink reuses keep-alive connections and preserves transport metadata", async () => {
  const received: { readonly body: string; readonly streamId: string | undefined }[] = [];
  let connections = 0;
  const server = createServer((request, response) => {
    void requestText(request)
      .then((body) => {
        received.push({
          body,
          streamId: firstHeader(request.headers["x-stream-id"])
        });
        response.statusCode = 200;
        response.end(`result:${body}`);
      })
      .catch((error: unknown) => {
        response.destroy(error instanceof Error ? error : new Error(String(error)));
      });
  });
  server.on("connection", () => {
    connections += 1;
  });
  await listen(server);
  const environment = new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(canonicalConfig())),
    1
  );
  const source = new ConsumedStream(sourceConfig, environment, makeTestSerde<string>());
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  const sink = new SinkStreamWithResult(sinkConfig, source);
  const downstream = new RecordingStream(resultConfig, environment);
  const client = new NodeHttpClient();
  const handler = new TestHandler(serverUrl(server));
  sink.setConsumer(downstream);
  makeNodeHttpEndpointConsumer(sink, client, handler);
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink);
  await dataSink.start(Context.background());

  try {
    await source.emit(new MessageContext().withStreamId("stream-1"), "one");
    await source.emit(new MessageContext().withStreamId("stream-2"), "two");

    assert.deepEqual(
      received.map(({ body }) => body),
      ["one", "two"]
    );
    const streamIds = received.map(({ streamId }) => streamId);
    assert.ok(streamIds.every((streamId) => streamId !== undefined));
    assert.equal(new Set(streamIds).size, 2);
    assert.ok(!streamIds.includes("stream-1"));
    assert.ok(!streamIds.includes("stream-2"));
    assert.deepEqual(downstream.values, ["result:one", "result:two"]);
    assert.equal(connections, 1);
    assert.deepEqual(handler.events, [
      "begin",
      "consume:one",
      "response:200",
      "end:ok",
      "begin",
      "consume:two",
      "response:200",
      "end:ok"
    ]);
  } finally {
    await dataSink.stop(Context.background());
    await close(server);
  }
});

await test("Node HTTP sink drains a backpressured streaming response", async () => {
  const chunk = Buffer.alloc(64 * 1_024, "x");
  const chunkCount = 64;
  let backpressured = false;
  const server = createServer((request, response) => {
    request.resume();
    void sendLargeResponse(response).catch((error: unknown) => {
      response.destroy(error instanceof Error ? error : new Error(String(error)));
    });
  });
  const sendLargeResponse = async (response: ServerResponse): Promise<void> => {
    response.statusCode = 200;
    response.setHeader("content-length", chunk.byteLength * chunkCount);
    for (let index = 0; index < chunkCount; index += 1) {
      if (!response.write(chunk)) {
        backpressured = true;
        await once(response, "drain");
      }
    }
    response.end();
  };
  await listen(server);
  const harness = makeSinkHarness(
    new TestHandler(serverUrl(server), chunk.byteLength * chunkCount),
    new NodeHttpClient()
  );
  await harness.dataSink.start(Context.background());
  try {
    await harness.source.emit(new MessageContext(), "value");
    assert.equal(backpressured, true);
    assert.equal(harness.result.values[0]?.length, chunk.byteLength * chunkCount);
  } finally {
    await harness.dataSink.stop(Context.background());
    await close(server);
  }
});

await test("Node HTTP sink preserves every canonical failure branch", async () => {
  const cases = [
    {
      mode: "begin" as const,
      expected: ["begin"],
      clientCalls: 0
    },
    {
      mode: "consume" as const,
      expected: ["begin", "consume:value", "end:consume failed"],
      clientCalls: 0
    },
    {
      mode: "missing-request" as const,
      expected: [
        "begin",
        "consume:value",
        "end:no HTTP request set by handler for sink endpoint processOrderItem"
      ],
      clientCalls: 0
    },
    {
      mode: "client" as const,
      expected: ["begin", "consume:value", "end:network failure"],
      clientCalls: 1
    }
  ];

  for (const item of cases) {
    const handler = new LifecycleHandler(item.mode);
    const client = new RejectingClient(new Error("network failure"));
    const harness = makeSinkHarness(handler, client);
    await harness.dataSink.start(Context.background());
    try {
      await harness.source.emit(new MessageContext(), "value");
      assert.deepEqual(handler.events, item.expected, item.mode);
      assert.equal(client.calls, item.clientCalls, item.mode);
    } finally {
      await harness.dataSink.stop(Context.background());
    }
    assert.equal(client.closed, true);
  }
});

await test("Node HTTP sink passes handleResponse errors to endRequest", async () => {
  const server = createServer((request, response) => {
    request.resume();
    response.statusCode = 200;
    response.end("ok");
  });
  await listen(server);
  const handler = new LifecycleHandler("handle", serverUrl(server));
  const client = new NodeHttpClient();
  const harness = makeSinkHarness(handler, client);
  await harness.dataSink.start(Context.background());
  try {
    await harness.source.emit(new MessageContext(), "value");
    assert.deepEqual(handler.events, ["begin", "consume:value", "handle", "end:handle failed"]);
  } finally {
    await harness.dataSink.stop(Context.background());
    await close(server);
  }
});

await test("Node HTTP sink cancels accepted calls at the shutdown deadline", async () => {
  let requestAccepted: (() => void) | undefined;
  const accepted = new Promise<void>((resolve) => {
    requestAccepted = resolve;
  });
  const server = createServer(() => {
    requestAccepted?.();
    requestAccepted = undefined;
  });
  await listen(server);
  const handler = new LifecycleHandler("client", serverUrl(server));
  const client = new NodeHttpClient();
  const harness = makeSinkHarness(handler, client);
  await harness.dataSink.start(Context.background());
  const operation = harness.source.emit(new MessageContext(), "value");
  try {
    await accepted;
    await harness.dataSink.stop(Context.background().bounded(5));
    await operation;
    assert.equal(handler.events[0], "begin");
    assert.equal(handler.events[1], "consume:value");
    assert.match(handler.events[2] ?? "", /^end:/);
  } finally {
    server.closeAllConnections();
    await harness.dataSink.stop(Context.background());
    await close(server);
  }
});

await test("Node HTTP sink records canonical endpoint metrics", async () => {
  const server = createServer((request, response) => {
    request.resume();
    response.statusCode = 200;
    response.end("ok");
  });
  await listen(server);
  const labels = {
    connector: "inventoryServiceApi",
    endpoint: "processOrderItem",
    protocol: ""
  };
  const metrics = new TestMetrics();
  const handler = new LifecycleHandler("handle", serverUrl(server));
  const harness = makeSinkHarness(handler, new NodeHttpClient(), metrics);
  await harness.dataSink.start(Context.background());
  try {
    await harness.source.emit(new MessageContext(), "value");
    assert.equal(metrics.counterValue("datasink_endpoint_messages_total", labels), 0);
    assert.equal(
      metrics.counterValue("datasink_endpoint_events_total", {
        ...labels,
        event: "request_error"
      }),
      1
    );
    assert.equal(metrics.gaugeValue("datasink_endpoint_active_requests", labels), 0);
    assert.equal(
      metrics.histogramValue("datasink_endpoint_request_duration_seconds", labels)?.count,
      1
    );
  } finally {
    await harness.dataSink.stop(Context.background());
    await close(server);
  }

  const beginMetrics = new TestMetrics();
  const beginHandler = new LifecycleHandler("begin");
  const client = new RejectingClient(new Error("must not run"));
  const beginHarness = makeSinkHarness(beginHandler, client, beginMetrics);
  await beginHarness.dataSink.start(Context.background());
  try {
    await beginHarness.source.emit(new MessageContext(), "value");
    assert.equal(client.calls, 0);
    assert.equal(
      beginMetrics.counterValue("datasink_endpoint_events_total", {
        ...labels,
        event: "begin_request_failed"
      }),
      1
    );
    assert.equal(beginMetrics.gaugeValue("datasink_endpoint_active_requests", labels), 0);
    assert.equal(
      beginMetrics.histogramValue("datasink_endpoint_request_duration_seconds", labels)?.count,
      0
    );
  } finally {
    await beginHarness.dataSink.stop(Context.background());
  }
});

await test("Node HTTP sink creates spans only for sampled messages", async () => {
  const server = createServer((request, response) => {
    void requestText(request).then((body) => {
      response.statusCode = 200;
      response.end(`result:${body}`);
    });
  });
  await listen(server);
  const tracing = new TestTracing();
  const handler = new TestHandler(serverUrl(server));
  const harness = makeSinkHarness(handler, new NodeHttpClient(), undefined, tracing);
  await harness.dataSink.start(Context.background());
  try {
    await harness.source.emit(new MessageContext(), "plain");
    assert.deepEqual(tracing.spans(), []);

    await harness.source.emit(new MessageContext().withSampling(true), "traced");
    const spans = tracing.spans();
    assert.equal(spans.filter(({ name }) => name === "http.output").length, 1);
    assert.ok(spans.some(({ name }) => name === "stream.sink"));
    assert.ok(spans.some(({ name }) => name === "stream.call"));
    const span = spans.find(({ name }) => name === "http.output");
    assert.ok(span);
    assert.equal(span.name, "http.output");
    assert.equal(span.tracerName, "orderservice");
    assert.deepEqual(
      span.events.map(({ name }) => name),
      ["begin_request", "consume_message", "http_call", "handle_response"]
    );
    assert.equal(traceAttribute(span.attributes, "stream"), "inventoryCall");
    assert.equal(traceAttribute(span.attributes, "endpoint"), "processOrderItem");
    assert.equal(traceAttribute(span.events[2]?.attributes ?? [], "status_code"), 200n);
  } finally {
    await harness.dataSink.stop(Context.background());
    await close(server);
  }
});

await test("Node HTTP sink marks the exact failed lifecycle stage on its span", async () => {
  const tracing = new TestTracing();
  const handler = new LifecycleHandler("client");
  const harness = makeSinkHarness(
    handler,
    new RejectingClient(new Error("network failure")),
    undefined,
    tracing
  );
  await harness.dataSink.start(Context.background());
  try {
    await harness.source.emit(new MessageContext().withSampling(true), "value");
    const span = tracing.spans()[0];
    assert.ok(span);
    assert.equal(span.statusCode, "error");
    assert.equal(span.statusDescription, "network failure");
    assert.equal(span.error?.message, "network failure");
    assert.deepEqual(
      span.events.map(({ name }) => name),
      ["begin_request", "consume_message", "http_call.error"]
    );
  } finally {
    await harness.dataSink.stop(Context.background());
  }
});

function traceAttribute(
  attributes: readonly { readonly key: string; readonly value: unknown }[],
  key: string
): unknown {
  return attributes.find((value) => value.key === key)?.value;
}

async function requestText(request: IncomingMessage): Promise<string> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of request) {
    const value: unknown = chunk;
    if (typeof value === "string") {
      chunks.push(Buffer.from(value));
    } else if (value instanceof Uint8Array) {
      chunks.push(value);
    } else {
      throw new TypeError("HTTP request emitted a non-byte chunk");
    }
  }
  return Buffer.concat(chunks).toString("utf8");
}

function firstHeader(value: string | readonly string[] | undefined): string | undefined {
  return typeof value === "string" ? value : value?.[0];
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
}

function serverUrl(server: Server): string {
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("test HTTP server has no TCP address");
  }
  return `http://127.0.0.1:${String(address.port)}/v1/processorderitem`;
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}
