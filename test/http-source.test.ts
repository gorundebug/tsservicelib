import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type Server } from "node:http";
import { connect as connectTcp, type Socket } from "node:net";
import { test } from "node:test";

import {
  type EndpointHandler,
  type HandlerData,
  type HTTPHandler,
  makeNodeHttpEndpointConsumer,
  type ResultContext
} from "@gorundebug/tsservicelib/datasource/http";
import { InputStream } from "@gorundebug/tsservicelib/operators";
import {
  type CanonicalConfig,
  ConsumedStream,
  Context,
  type MessageContext,
  RuntimeConfig,
  RuntimeConfigStore,
  ServiceEnvironment,
  ServiceStream,
  type StreamContext,
  type StreamConfig,
  type HttpDataConnectorConfig,
  type HttpEndpointConfig,
  type Metrics,
  stringSerdeType,
  type Tracing,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestMetrics } from "@gorundebug/tsservicelib/runtime/testmetrics";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";
import { makeTestSerde } from "./support/environment.js";

const inputConfig = {
  id: 1,
  name: "processOrder",
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
} as const;

const downstreamConfig: StreamConfig = {
  id: 2,
  name: "downstream",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0
};

const resultConfig: StreamConfig = {
  ...downstreamConfig,
  id: 3,
  name: "result"
};

function canonicalConfig(
  httpPort = 9091
): CanonicalConfig<HttpDataConnectorConfig, HttpEndpointConfig> {
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
        httpPort,
        metricsHandler: "/metrics",
        shutdownTimeout: 1_000,
        statusHandler: "/status",
        startupHandler: "/health/startup",
        readinessHandler: "/health/ready",
        livenessHandler: "/health/live",
        kubernetesWorkloadType: "Deployment"
      }
    ],
    streams: [inputConfig, downstreamConfig, resultConfig],
    dataConnectors: [
      {
        id: 10,
        name: "orderServiceApi",
        type: 1,
        implementation: "typescript/nodeHttp",
        host: "127.0.0.1",
        port: 9091,
        useDedicatedListener: false,
        properties: {}
      }
    ],
    endpoints: [
      {
        id: 100,
        name: "processOrder",
        idDataConnector: 10,
        httpMethodType: "POST",
        path: "/v1/processorder",
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

function environment(metrics?: Metrics, tracing?: Tracing, httpPort?: number): ServiceEnvironment {
  return new ServiceEnvironment(
    new RuntimeConfigStore(new RuntimeConfig(canonicalConfig(httpPort))),
    1,
    undefined,
    undefined,
    undefined,
    undefined,
    metrics,
    tracing
  );
}

class RecordingStream extends ServiceStream implements TypedStreamConsumer<string> {
  public readonly values: { readonly context: MessageContext; readonly value: string }[] = [];

  public consume(context: MessageContext, value: string): void {
    this.values.push({ context, value });
  }
}

interface NoResultState {
  readonly kind: "noResult";
}

class NoResultHandler implements EndpointHandler<
  NoResultState,
  string,
  string,
  string,
  string,
  Error
> {
  public readonly events: string[] = [];

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: NoResultState }> {
    this.events.push("begin");
    return Promise.resolve({ context, state: { kind: "noResult" } });
  }

  public async consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: NoResultState,
    data: HandlerData
  ): Promise<void> {
    void state;
    this.events.push("consume");
    await stream.collect(context, await requestText(data.request));
    data.response.statusCode = 200;
    data.response.end("ok");
  }

  public getMessageId(): string {
    return "";
  }

  public endRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    error: Error | undefined
  ): void {
    void context;
    void stream;
    this.events.push(error === undefined ? "end:ok" : `end:${error.message}`);
  }
}

class ResultHandler implements EndpointHandler<
  { id: string },
  string,
  string,
  string,
  string,
  Error
> {
  public readonly events: string[] = [];

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: { readonly id: string } }> {
    this.events.push("begin");
    return Promise.resolve({ context, state: { id: "message-1" } });
  }

  public async consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: { readonly id: string },
    data: HandlerData,
    resultContext: ResultContext<{ id: string }, string, string, string, string, Error>
  ): Promise<void> {
    this.events.push("consume");
    resultContext.setResultCallback(
      state.id,
      (resultContextValue, resultStream, resultState, value, resultData) => {
        void resultContextValue;
        void resultStream;
        void resultState;
        this.events.push("callback");
        resultData.response.statusCode = 200;
        resultData.response.end(value);
        resultContext.done();
        return true;
      }
    );
    await stream.collect(context, await requestText(data.request));
  }

  public getMessageId(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: { readonly id: string }
  ): string {
    void context;
    void stream;
    return state.id;
  }

  public endRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    error: Error | undefined
  ): void {
    void context;
    void stream;
    this.events.push(error === undefined ? "end:ok" : `end:${error.message}`);
  }
}

class BeginFailureHandler implements EndpointHandler<
  NoResultState,
  string,
  string,
  string,
  string,
  Error
> {
  public readonly events: string[] = [];

  public beginRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    data: HandlerData
  ): Promise<{ readonly context: MessageContext; readonly state: NoResultState }> {
    void context;
    void stream;
    this.events.push("begin");
    data.response.statusCode = 418;
    data.response.end("begin failed");
    return Promise.reject(new Error("begin failed"));
  }

  public consumeMessage(): never {
    throw new Error("consumeMessage must not be called after beginRequest failure");
  }

  public getMessageId(): string {
    return "";
  }

  public endRequest(): void {
    this.events.push("end");
  }
}

class CancellationHandler implements EndpointHandler<
  NoResultState,
  string,
  string,
  string,
  string,
  Error
> {
  public callbacks = 0;
  public endError: Error | undefined;
  readonly #entered: Promise<void>;
  #resolveEntered: (() => void) | undefined;
  readonly #ended: Promise<void>;
  #resolveEnded: (() => void) | undefined;

  public constructor() {
    this.#entered = new Promise((resolve) => {
      this.#resolveEntered = resolve;
    });
    this.#ended = new Promise((resolve) => {
      this.#resolveEnded = resolve;
    });
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: NoResultState }> {
    return Promise.resolve({ context, state: { kind: "noResult" } });
  }

  public async consumeMessage(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    state: NoResultState,
    data: HandlerData,
    resultContext: ResultContext<NoResultState, string, string, string, string, Error>
  ): Promise<void> {
    void state;
    void data;
    resultContext.setResultCallback("message", () => {
      this.callbacks += 1;
      return true;
    });
    await stream.collect(context, "accepted");
    this.#resolveEntered?.();
    this.#resolveEntered = undefined;
  }

  public getMessageId(): string {
    return "message";
  }

  public endRequest(
    context: MessageContext,
    stream: StreamContext<string, string, Error>,
    error: Error | undefined
  ): void {
    void context;
    void stream;
    this.endError = error;
    this.#resolveEnded?.();
    this.#resolveEnded = undefined;
  }

  public entered(): Promise<void> {
    return this.#entered;
  }

  public ended(): Promise<void> {
    return this.#ended;
  }
}

class PipelineResultStream extends ServiceStream implements TypedStreamConsumer<string> {
  readonly #result: ConsumedStream<string>;

  public constructor(environment: ServiceEnvironment, result: ConsumedStream<string>) {
    super(downstreamConfig, environment);
    this.#result = result;
  }

  public consume(context: MessageContext, value: string): Promise<void> {
    return Promise.resolve(this.#result.emit(context, `result:${value}`));
  }
}

await test("non-dedicated Node HTTP source uses the service HTTP listener", async () => {
  const port = await availablePort();
  const env = environment(undefined, undefined, port);
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  const downstream = new RecordingStream(downstreamConfig, env);
  input.setConsumer(downstream);
  makeNodeHttpEndpointConsumer(input, new NoResultHandler());
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  await env.httpServer().start(Context.background());
  try {
    const response = await fetch(`http://127.0.0.1:${String(port)}/v1/processorder`, {
      method: "POST",
      body: "shared-listener"
    });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "ok");
    assert.equal(downstream.values[0]?.value, "shared-listener");
  } finally {
    await env.httpServer().stop(Context.background());
    await source.stop(Context.background());
  }
});

await test("Node HTTP source preserves handler lifecycle, method gate and stream ID", async () => {
  const env = environment();
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  const downstream = new RecordingStream(downstreamConfig, env);
  const endpointHandler = new NoResultHandler();
  input.setConsumer(downstream);
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  try {
    const response = await fetch(url(server), {
      method: "POST",
      headers: { "x-stream-id": "stream-1" },
      body: "order-1"
    });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "ok");
    assert.deepEqual(endpointHandler.events, ["begin", "consume", "end:ok"]);
    const received = downstream.values[0];
    assert.ok(received);
    assert.equal(received.value, "order-1");
    assert.equal(received.context.streamId(), "stream-1");
    assert.equal(received.context.cancelled(), true);

    const rejected = await fetch(url(server), { method: "GET" });
    assert.equal(rejected.status, 405);
    assert.equal(rejected.headers.get("allow"), "POST");
    assert.deepEqual(endpointHandler.events, ["begin", "consume", "end:ok"]);
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("service HTTP listener rejects malformed headers before framework dispatch", async () => {
  const port = await availablePort();
  const env = environment(undefined, undefined, port);
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  input.setConsumer(new RecordingStream(downstreamConfig, env));
  const endpointHandler = new NoResultHandler();
  makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  await env.httpServer().start(Context.background());
  try {
    const response = await rawHttpExchange(
      port,
      "POST /v1/processorder HTTP/1.1\r\nHost: localhost\r\nMalformed Header\r\n\r\n"
    );
    assert.match(response, /^HTTP\/1\.1 400 /);
    assert.deepEqual(endpointHandler.events, []);
  } finally {
    await env.httpServer().stop(Context.background());
    await source.stop(Context.background());
  }
});

await test("service HTTP shutdown bounds an incomplete slowloris request", async () => {
  const port = await availablePort();
  const env = environment(undefined, undefined, port);
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  input.setConsumer(new RecordingStream(downstreamConfig, env));
  const endpointHandler = new NoResultHandler();
  makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  await env.httpServer().start(Context.background());
  const socket = await openSocket(port);
  try {
    socket.write("POST /v1/processorder HTTP/1.1\r\nHost: localhost\r\nX-Slow: ");
    const started = performance.now();
    await env.httpServer().stop(Context.background().bounded(25));
    assert.ok(performance.now() - started < 500, "shutdown must not wait for header timeout");
    assert.deepEqual(endpointHandler.events, []);
  } finally {
    socket.destroy();
    await source.stop(Context.background());
  }
});

await test("Node HTTP source correlates an asynchronous pipeline result and retires once", async () => {
  const env = environment();
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  const result = new ConsumedStream(resultConfig, env, env.serde(stringSerdeType));
  const pipeline = new PipelineResultStream(env, result);
  const endpointHandler = new ResultHandler();
  input.setConsumer(pipeline);
  input.setSource(result);
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  try {
    const response = await fetch(url(server), { method: "POST", body: "order-2" });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "result:order-2");
    assert.deepEqual(endpointHandler.events, ["begin", "consume", "callback", "end:ok"]);
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("Node HTTP source skips consume and end when beginRequest fails", async () => {
  const env = environment();
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  input.setConsumer(new RecordingStream(downstreamConfig, env));
  const endpointHandler = new BeginFailureHandler();
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  try {
    const response = await fetch(url(server), { method: "POST" });
    assert.equal(response.status, 418);
    assert.equal(await response.text(), "begin failed");
    assert.deepEqual(endpointHandler.events, ["begin"]);
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("Node HTTP source cancellation retires the request before a late result", async () => {
  const env = environment();
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  const accepted = new RecordingStream(downstreamConfig, env);
  const result = new ConsumedStream(resultConfig, env, env.serde(stringSerdeType));
  const endpointHandler = new CancellationHandler();
  input.setConsumer(accepted);
  input.setSource(result);
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  const controller = new AbortController();
  try {
    const request = fetch(url(server), { method: "POST", signal: controller.signal });
    await endpointHandler.entered();
    controller.abort(new Error("client cancelled"));
    await assert.rejects(request);
    await endpointHandler.ended();
    assert.match(endpointHandler.endError?.message ?? "", /disconnect|cancel|abort/i);
    const requestContext = accepted.values[0]?.context;
    assert.ok(requestContext);
    await result.emit(requestContext, "late");
    assert.equal(endpointHandler.callbacks, 0);
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("Node HTTP source closes admission and cancels accepted work at shutdown deadline", async () => {
  const env = environment();
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  const accepted = new RecordingStream(downstreamConfig, env);
  const result = new ConsumedStream(resultConfig, env, env.serde(stringSerdeType));
  const endpointHandler = new CancellationHandler();
  input.setConsumer(accepted);
  input.setSource(result);
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  const controller = new AbortController();
  const request = fetch(url(server), { method: "POST", signal: controller.signal });
  try {
    await endpointHandler.entered();
    await source.stop(Context.background().bounded(5));
    await endpointHandler.ended();
    assert.ok(endpointHandler.endError);

    const rejected = await fetch(url(server), { method: "POST" });
    assert.equal(rejected.status, 503);
  } finally {
    controller.abort();
    await assert.rejects(request);
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("Node HTTP source records canonical endpoint metrics", async () => {
  const metrics = new TestMetrics();
  const env = environment(metrics);
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  input.setConsumer(new RecordingStream(downstreamConfig, env));
  const endpointHandler = new NoResultHandler();
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  const labels = { connector: "orderServiceApi", endpoint: "processOrder", protocol: "" };
  try {
    const response = await fetch(url(server), { method: "POST", body: "order" });
    assert.equal(response.status, 200);
    await response.text();
    const rejected = await fetch(url(server), { method: "GET" });
    assert.equal(rejected.status, 405);

    assert.equal(metrics.counterValue("datasource_endpoint_messages_total", labels), 1);
    assert.equal(metrics.gaugeValue("datasource_endpoint_active_requests", labels), 0);
    assert.equal(metrics.gaugeValue("datasource_endpoint_pending_requests", labels), 0);
    assert.equal(
      metrics.counterValue("datasource_endpoint_events_total", {
        ...labels,
        event: "invalid_http_method"
      }),
      1
    );
    assert.equal(
      metrics.histogramValue("datasource_endpoint_request_duration_seconds", labels)?.count,
      1
    );
    assert.equal(
      metrics.observableGaugeValue("datasource_endpoint_pending_oldest_age_seconds", labels),
      0
    );
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("Node HTTP source creates spans only for sampled requests", async () => {
  const tracing = new TestTracing();
  const env = environment(undefined, tracing);
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  input.setConsumer(new RecordingStream(downstreamConfig, env));
  const [, handler] = makeNodeHttpEndpointConsumer(input, new NoResultHandler());
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  try {
    const unsampled = await fetch(url(server), { method: "POST", body: "plain" });
    await unsampled.text();
    assert.deepEqual(tracing.spans(), []);

    const sampled = await fetch(url(server), {
      method: "POST",
      headers: { "x-trace": "1" },
      body: "traced"
    });
    await sampled.text();
    const spans = tracing.spans();
    assert.equal(spans.filter(({ name }) => name === "http.input").length, 1);
    assert.ok(spans.some(({ name }) => name === "stream.input"));
    assert.ok(spans.some(({ name }) => name === "stream.call"));
    const span = spans.find(({ name }) => name === "http.input");
    assert.ok(span);
    assert.equal(span.name, "http.input");
    assert.equal(span.tracerName, "orderservice");
    assert.deepEqual(
      span.events.map(({ name }) => name),
      ["begin_request", "consume_message"]
    );
    assert.equal(attribute(span.attributes, "stream"), "processOrder");
    assert.equal(attribute(span.attributes, "endpoint"), "processOrder");
    assert.equal(attribute(span.attributes, "method"), "POST");
    assert.equal(attribute(span.attributes, "path"), "/v1/processorder");
    assert.equal(attribute(span.attributes, "has_result"), false);
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

await test("Node HTTP source records beginRequest failure without calling endRequest", async () => {
  const tracing = new TestTracing();
  const env = environment(undefined, tracing);
  const input = new InputStream<string, string, Error>(
    inputConfig,
    env,
    makeTestSerde(),
    makeTestSerde()
  );
  input.setConsumer(new RecordingStream(downstreamConfig, env));
  const endpointHandler = new BeginFailureHandler();
  const [, handler] = makeNodeHttpEndpointConsumer(input, endpointHandler);
  const source = env.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const server = await startServer(handler);
  try {
    const response = await fetch(url(server), {
      method: "POST",
      headers: { "x-trace": "1" }
    });
    await response.text();
    assert.deepEqual(endpointHandler.events, ["begin"]);
    const span = tracing.spans()[0];
    assert.ok(span);
    assert.equal(span.statusCode, "error");
    assert.equal(span.statusDescription, "begin failed");
    assert.deepEqual(
      span.events.map(({ name }) => name),
      ["begin_request.error"]
    );
  } finally {
    await source.stop(Context.background());
    await stopServer(server);
  }
});

function attribute(
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

function startServer(handler: HTTPHandler): Promise<Server> {
  const server = createServer(handler);
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve(server);
    });
  });
}

function url(server: Server): string {
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("test HTTP server has no TCP address");
  }
  return `http://127.0.0.1:${String(address.port)}/v1/processorder`;
}

function stopServer(server: Server): Promise<void> {
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

async function availablePort(): Promise<number> {
  const server = await startServer((_request, response) => {
    response.end();
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    await stopServer(server);
    throw new Error("test HTTP server has no TCP address");
  }
  const { port } = address;
  await stopServer(server);
  return port;
}

function openSocket(port: number): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connectTcp(port, "127.0.0.1");
    const connected = (): void => {
      cleanup();
      resolve(socket);
    };
    const failed = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const cleanup = (): void => {
      socket.removeListener("connect", connected);
      socket.removeListener("error", failed);
    };
    socket.once("connect", connected);
    socket.once("error", failed);
  });
}

async function rawHttpExchange(port: number, request: string): Promise<string> {
  const socket = await openSocket(port);
  return new Promise((resolve, reject) => {
    const chunks: Uint8Array[] = [];
    const received = (chunk: Uint8Array): void => {
      chunks.push(chunk);
    };
    const completed = (): void => {
      cleanup();
      resolve(Buffer.concat(chunks).toString("utf8"));
    };
    const failed = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const cleanup = (): void => {
      socket.removeListener("data", received);
      socket.removeListener("end", completed);
      socket.removeListener("close", completed);
      socket.removeListener("error", failed);
    };
    socket.on("data", received);
    socket.once("end", completed);
    socket.once("close", completed);
    socket.once("error", failed);
    socket.end(request);
  });
}
