import assert from "node:assert/strict";
import { test } from "node:test";

import {
  create,
  fromBinary,
  toBinary,
  type DescMethod,
  type DescService
} from "@bufbuild/protobuf";
import { TimestampSchema, type Timestamp } from "@bufbuild/protobuf/wkt";
import { Client, Metadata, credentials } from "@grpc/grpc-js";

import {
  type EndpointHandler,
  type ResultContext,
  type Sender,
  makeGrpcNoStreamingEndpointConsumer
} from "@gorundebug/tsservicelib/datasource/grpc";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  type DataSource,
  ServiceStream,
  errorSerdeType,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig,
  type InputStreamConfig,
  type MessageContext,
  type StreamConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

const inputConfig: InputStreamConfig = {
  id: 1,
  name: "input",
  properties: {},
  type: "Input",
  pipeline: "main",
  idService: 1,
  idSource: 3,
  idSources: [],
  xPos: 0,
  yPos: 0,
  idEndpoint: 100,
  valueType: "timestamp"
};
const resultConfig: StreamConfig = {
  id: 3,
  name: "result",
  properties: {},
  type: "Map",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0
};
const feedbackConfig: StreamConfig = { ...resultConfig, id: 4, name: "feedback" };
const connector: GrpcDataConnectorConfig = {
  id: 10,
  name: "echo",
  properties: {},
  type: 2,
  implementation: "grpc/grpc-js",
  connectionsCount: 1
};
const endpoint: GrpcEndpointConfig = {
  id: 100,
  name: "echo",
  properties: {},
  idDataConnector: 10,
  grpcMethodType: "NoStreaming",
  methodName: "Echo"
};

function descriptors(): readonly [DescService, DescMethod] {
  const service = {
    kind: "service",
    typeName: "test.EchoService",
    name: "EchoService",
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => "test.EchoService"
  } as unknown as DescService;
  const method = {
    kind: "rpc",
    name: "Echo",
    localName: "echo",
    parent: service,
    methodKind: "unary",
    input: TimestampSchema,
    output: TimestampSchema,
    deprecated: false,
    idempotency: undefined,
    proto: {},
    toString: () => "test.EchoService.Echo"
  } as unknown as DescMethod;
  service.methods.push(method);
  service.method["echo"] = method;
  return [service, method];
}

class Feedback extends ServiceStream implements TypedStreamConsumer<Timestamp> {
  readonly #result: ConsumedStream<Timestamp>;
  public constructor(result: ConsumedStream<Timestamp>) {
    super(feedbackConfig, result.runtimeEnvironment());
    this.#result = result;
  }
  public consume(context: MessageContext, value: Timestamp): Promise<void> {
    return Promise.resolve(this.#result.emit(context, value));
  }
}

class Handler implements EndpointHandler<
  undefined,
  Timestamp,
  Timestamp,
  Timestamp,
  Timestamp,
  Error
> {
  public context: MessageContext | undefined;

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: undefined }> {
    this.context = context;
    return Promise.resolve({ context, state: undefined });
  }
  public consumeMessage(
    context: MessageContext,
    stream: StreamContext<Timestamp, Timestamp, Error>,
    _state: undefined,
    request: Timestamp,
    result: ResultContext<undefined, Timestamp, Timestamp, Timestamp, Error>,
    sender: Sender<Timestamp>
  ): Promise<void> {
    result.setResultCallback("echo", async (resultContext, _stream, _handlerState, value) => {
      await sender.send(resultContext, value);
      result.done();
      return true;
    });
    return Promise.resolve(stream.collect(context, request));
  }
  public getMessageId(): string {
    return "echo";
  }
  public eof(): void {
    return;
  }
  public endRequest(): void {
    return;
  }
}

class DoubleResponseHandler implements EndpointHandler<
  undefined,
  Timestamp,
  Timestamp,
  Timestamp,
  Timestamp,
  Error
> {
  public secondResponseError: Error | undefined;

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: undefined }> {
    return Promise.resolve({ context, state: undefined });
  }

  public async consumeMessage(
    context: MessageContext,
    _stream: StreamContext<Timestamp, Timestamp, Error>,
    _state: undefined,
    request: Timestamp,
    _result: ResultContext<undefined, Timestamp, Timestamp, Timestamp, Error>,
    sender: Sender<Timestamp>
  ): Promise<void> {
    await sender.send(context, request);
    try {
      await sender.send(context, request);
    } catch (error: unknown) {
      this.secondResponseError = error instanceof Error ? error : new Error(String(error));
    }
  }

  public getMessageId(): string {
    return "echo";
  }

  public eof(): void {
    return;
  }

  public endRequest(): void {
    return;
  }
}

class ConcurrentHandler implements EndpointHandler<
  undefined,
  Timestamp,
  Timestamp,
  Timestamp,
  Timestamp,
  Error
> {
  readonly #expected: number;
  readonly #ready: Promise<void>;
  #release: (() => void) | undefined;
  #active = 0;
  public maxActive = 0;

  public constructor(expected: number) {
    this.#expected = expected;
    this.#ready = new Promise((resolve) => {
      this.#release = resolve;
    });
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: undefined }> {
    return Promise.resolve({ context, state: undefined });
  }

  public async consumeMessage(
    context: MessageContext,
    _stream: StreamContext<Timestamp, Timestamp, Error>,
    _state: undefined,
    request: Timestamp,
    _result: ResultContext<undefined, Timestamp, Timestamp, Timestamp, Error>,
    sender: Sender<Timestamp>
  ): Promise<void> {
    this.#active += 1;
    this.maxActive = Math.max(this.maxActive, this.#active);
    if (this.#active === this.#expected) this.#release?.();
    await this.#ready;
    try {
      await sender.send(context, request);
    } finally {
      this.#active -= 1;
    }
  }

  public getMessageId(): string {
    return "echo";
  }

  public eof(): void {
    return;
  }

  public endRequest(): void {
    return;
  }
}

class ControlledHandler implements EndpointHandler<
  undefined,
  Timestamp,
  Timestamp,
  Timestamp,
  Timestamp,
  Error
> {
  public readonly entered: Promise<void>;
  readonly #released: Promise<void>;
  #markEntered: (() => void) | undefined;
  #release: (() => void) | undefined;
  public endError: Error | undefined;
  public endCalls = 0;

  public constructor() {
    this.entered = new Promise((resolve) => {
      this.#markEntered = resolve;
    });
    this.#released = new Promise((resolve) => {
      this.#release = resolve;
    });
  }

  public release(): void {
    this.#release?.();
    this.#release = undefined;
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: undefined }> {
    return Promise.resolve({ context, state: undefined });
  }

  public async consumeMessage(
    context: MessageContext,
    _stream: StreamContext<Timestamp, Timestamp, Error>,
    _state: undefined,
    request: Timestamp,
    _result: ResultContext<undefined, Timestamp, Timestamp, Timestamp, Error>,
    sender: Sender<Timestamp>
  ): Promise<void> {
    this.#markEntered?.();
    this.#markEntered = undefined;
    await Promise.race([
      this.#released,
      new Promise<never>((_resolve, reject) => {
        const cancelled = (): void => {
          reject(new Error("gRPC call cancelled"));
        };
        if (context.cancelled()) cancelled();
        else context.signal().addEventListener("abort", cancelled, { once: true });
      })
    ]);
    await sender.send(context, request);
  }

  public getMessageId(): string {
    return "echo";
  }

  public eof(): void {
    return;
  }

  public endRequest(
    _context: MessageContext,
    _stream: StreamContext<Timestamp, Timestamp, Error>,
    error: Error | undefined
  ): void {
    this.endCalls += 1;
    this.endError = error;
  }
}

function unaryCall(client: Client, request: Timestamp): Promise<Timestamp> {
  return new Promise((resolve, reject) => {
    client.makeUnaryRequest(
      "/test.EchoService/Echo",
      (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
      request,
      { deadline: Date.now() + 1_000 },
      (error, value) => {
        if (error !== null) reject(error);
        else if (value === undefined) reject(new Error("gRPC call returned no value"));
        else resolve(value);
      }
    );
  });
}

async function makeUnarySource(
  port: number,
  handler: EndpointHandler<undefined, Timestamp, Timestamp, Timestamp, Timestamp, Error>
): Promise<{
  readonly source: DataSource;
  readonly client: Client;
}> {
  const directInputConfig: InputStreamConfig = { ...inputConfig, idSource: 0 };
  const environment = makeTestEnvironment([directInputConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint],
    service: { grpcPort: port }
  });
  registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  environment.serdeRegistry().registerStreamErrorType(directInputConfig.id, errorSerdeType);
  const input = makeInputStream<Timestamp, Timestamp, Error>(directInputConfig, environment);
  const [service, method] = descriptors();
  makeGrpcNoStreamingEndpointConsumer(input, service, method, handler);
  const source = environment.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  return { source, client: new Client(`127.0.0.1:${String(port)}`, credentials.createInsecure()) };
}

await test("gRPC unary source carries a pipeline result back to the client", async () => {
  const tracing = new TestTracing();
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint],
    service: { grpcPort: 19201 },
    tracing
  });
  const timestampType = registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<Timestamp, Timestamp, Error>(inputConfig, environment);
  const result = new ConsumedStream(resultConfig, environment, environment.serde(timestampType));
  input.setSource(result);
  input.setConsumer(new Feedback(result));
  const [service, method] = descriptors();
  const handler = new Handler();
  makeGrpcNoStreamingEndpointConsumer(input, service, method, handler);
  const source = environment.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const client = new Client("127.0.0.1:19201", credentials.createInsecure());
  try {
    const request = create(TimestampSchema, { seconds: 42n, nanos: 7 });
    const metadata = new Metadata();
    metadata.set("x-stream-id", "transport-stream");
    metadata.set("x-trace", "1");
    metadata.set("baggage", "tenant=alpha");
    metadata.set("x-private", "must-not-cross-runtime-boundary");
    const response = await new Promise<Timestamp>((resolve, reject) => {
      client.makeUnaryRequest(
        "/test.EchoService/Echo",
        (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
        (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
        request,
        metadata,
        { deadline: Date.now() + 1_000 },
        (error, value) => {
          if (error !== null) reject(error);
          else if (value === undefined) reject(new Error("gRPC call returned no value"));
          else resolve(value);
        }
      );
    });
    assert.equal(response.seconds, 42n);
    assert.equal(response.nanos, 7);
    const receivedContext = handler.context;
    assert.ok(receivedContext);
    assert.equal(receivedContext.streamId(), "transport-stream");
    assert.equal(receivedContext.samplingEnabled(), true);
    assert.equal(receivedContext.metadata().get("baggage"), "tenant=alpha");
    assert.equal(receivedContext.metadata().has("x-private"), false);
    assert.ok((receivedContext.remainingMs() ?? 0) > 0);
    assert.ok((receivedContext.remainingMs() ?? Infinity) <= 1_000);
    const spans = tracing.spans().filter(({ name }) => name === "grpc.input");
    assert.equal(spans.length, 1);
    assert.deepEqual(
      spans[0]?.events.map(({ name }) => name),
      ["begin_request", "send", "result_consumed", "consume_message", "eof", "result_received"]
    );
  } finally {
    client.close();
    await source.stop(Context.background());
  }
});

await test("gRPC unary source rejects a second response like the Go buffered sender", async () => {
  const environment = makeTestEnvironment([inputConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint],
    service: { grpcPort: 19205 }
  });
  registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  environment.serdeRegistry().registerStreamErrorType(inputConfig.id, errorSerdeType);
  const input = makeInputStream<Timestamp, Timestamp, Error>(inputConfig, environment);
  const result = new ConsumedStream(
    resultConfig,
    environment,
    environment.serdeByName<Timestamp>("timestamp")
  );
  input.setSource(result);
  const [service, method] = descriptors();
  const handler = new DoubleResponseHandler();
  makeGrpcNoStreamingEndpointConsumer(input, service, method, handler);
  const source = environment.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const client = new Client("127.0.0.1:19205", credentials.createInsecure());
  try {
    const request = create(TimestampSchema, { seconds: 7n, nanos: 1 });
    const response = await new Promise<Timestamp>((resolve, reject) => {
      client.makeUnaryRequest(
        "/test.EchoService/Echo",
        (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
        (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
        request,
        (error, value) => {
          if (error !== null) reject(error);
          else if (value === undefined) reject(new Error("gRPC call returned no value"));
          else resolve(value);
        }
      );
    });
    assert.equal(response.seconds, 7n);
    assert.match(handler.secondResponseError?.message ?? "", /already sent/);
  } finally {
    client.close();
    await source.stop(Context.background());
  }
});

await test("gRPC source dispatches accepted unary calls concurrently", async () => {
  const concurrentInputConfig: InputStreamConfig = { ...inputConfig, idSource: 0 };
  const environment = makeTestEnvironment([concurrentInputConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint],
    service: { grpcPort: 19206 }
  });
  registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  environment.serdeRegistry().registerStreamErrorType(concurrentInputConfig.id, errorSerdeType);
  const input = makeInputStream<Timestamp, Timestamp, Error>(concurrentInputConfig, environment);
  const [service, method] = descriptors();
  const handler = new ConcurrentHandler(4);
  makeGrpcNoStreamingEndpointConsumer(input, service, method, handler);
  const source = environment.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const client = new Client("127.0.0.1:19206", credentials.createInsecure());
  try {
    const responses = await Promise.all(
      [1n, 2n, 3n, 4n].map((seconds) =>
        unaryCall(client, create(TimestampSchema, { seconds, nanos: 0 }))
      )
    );
    assert.deepEqual(
      responses.map((response) => response.seconds),
      [1n, 2n, 3n, 4n]
    );
    assert.equal(handler.maxActive, 4);
  } finally {
    client.close();
    await source.stop(Context.background());
  }
});

await test("gRPC source closes admission and gracefully drains an accepted call", async () => {
  const handler = new ControlledHandler();
  const { source, client } = await makeUnarySource(19207, handler);
  const request = create(TimestampSchema, { seconds: 8n, nanos: 0 });
  try {
    const response = unaryCall(client, request);
    await handler.entered;
    let stopped = false;
    const stopping = source.stop(Context.background()).then(() => {
      stopped = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(stopped, false);
    handler.release();
    assert.equal((await response).seconds, 8n);
    await stopping;
    assert.equal(handler.endCalls, 1);
    assert.equal(handler.endError, undefined);
    await assert.rejects(unaryCall(client, request));
  } finally {
    client.close();
    await source.stop(Context.background());
  }
});

await test("gRPC source force-cancels an accepted call at the shutdown deadline", async () => {
  const handler = new ControlledHandler();
  const { source, client } = await makeUnarySource(19208, handler);
  const request = create(TimestampSchema, { seconds: 9n, nanos: 0 });
  const outcome = unaryCall(client, request).then(
    () => undefined,
    (error: unknown) => error
  );
  try {
    await handler.entered;
    await source.stop(Context.background().bounded(10));
    assert.ok((await outcome) instanceof Error);
    for (let attempt = 0; handler.endCalls === 0 && attempt < 20; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    assert.equal(handler.endCalls, 1);
    assert.match(handler.endError?.message ?? "", /cancel/i);
  } finally {
    client.close();
    await source.stop(Context.background());
  }
});
