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
import { Client, Metadata, credentials, status as grpcStatus } from "@grpc/grpc-js";

import {
  makeGrpcBidiStreamingEndpointConsumer,
  makeGrpcClientStreamingEndpointConsumer,
  makeGrpcServerStreamingEndpointConsumer,
  type EndpointHandler,
  type ResultContext,
  type Sender
} from "@gorundebug/tsservicelib/datasource/grpc";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  Context,
  ServiceStream,
  errorSerdeType,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig,
  type InputStreamConfig,
  type MessageContext,
  type StreamContext,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";

interface HandlerState {
  last: Timestamp | undefined;
  sender: Sender<Timestamp> | undefined;
}

class RecordingConsumer extends ServiceStream implements TypedStreamConsumer<Timestamp> {
  readonly values: bigint[] = [];

  public consume(_context: MessageContext, value: Timestamp): void {
    this.values.push(value.seconds);
  }
}

class StreamingHandler implements EndpointHandler<
  HandlerState,
  Timestamp,
  Timestamp,
  Timestamp,
  Timestamp,
  Error
> {
  readonly #clientStreaming: boolean;
  readonly lifecycle: string[] = [];

  public constructor(clientStreaming: boolean) {
    this.#clientStreaming = clientStreaming;
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: HandlerState }> {
    this.lifecycle.push("begin");
    return Promise.resolve({ context, state: { last: undefined, sender: undefined } });
  }

  public async consumeMessage(
    context: MessageContext,
    stream: StreamContext<Timestamp, Timestamp, Error>,
    state: HandlerState,
    request: Timestamp,
    _result: ResultContext<HandlerState, Timestamp, Timestamp, Timestamp, Error>,
    sender: Sender<Timestamp>
  ): Promise<void> {
    this.lifecycle.push("consume");
    state.last = request;
    state.sender = sender;
    await stream.collect(context, request);
    if (!this.#clientStreaming) await sender.send(context, request);
  }

  public getMessageId(): string {
    return "unused";
  }

  public async eof(
    context: MessageContext,
    _stream: StreamContext<Timestamp, Timestamp, Error>,
    state: HandlerState
  ): Promise<void> {
    this.lifecycle.push("eof");
    if (this.#clientStreaming && state.last !== undefined && state.sender !== undefined) {
      await state.sender.send(context, state.last);
    }
  }

  public endRequest(): void {
    this.lifecycle.push("end");
  }
}

const connector: GrpcDataConnectorConfig = {
  id: 10,
  name: "streaming",
  properties: {},
  type: 2,
  implementation: "grpc/grpc-js",
  connectionsCount: 1
};

function descriptors(): readonly [DescService, DescMethod, DescMethod, DescMethod] {
  const service = {
    kind: "service",
    typeName: "test.StreamingService",
    name: "StreamingService",
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => "test.StreamingService"
  } as unknown as DescService;
  const method = (name: string, localName: string, methodKind: DescMethod["methodKind"]) =>
    ({
      kind: "rpc",
      name,
      localName,
      parent: service,
      methodKind,
      input: TimestampSchema,
      output: TimestampSchema,
      deprecated: false,
      idempotency: undefined,
      proto: {},
      toString: () => `test.StreamingService.${name}`
    }) as unknown as DescMethod;
  const client = method("Client", "client", "client_streaming");
  const server = method("Server", "server", "server_streaming");
  const bidi = method("Bidi", "bidi", "bidi_streaming");
  service.methods.push(client, server, bidi);
  service.method["client"] = client;
  service.method["server"] = server;
  service.method["bidi"] = bidi;
  return [service, client, server, bidi];
}

async function within<T>(label: string | (() => string), promise: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${typeof label === "string" ? label : label()} timed out`));
        }, 2_000);
      })
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

await test("gRPC streaming sources preserve the canonical request lifecycle", async () => {
  const endpointConfigs: GrpcEndpointConfig[] = [
    {
      id: 101,
      name: "client",
      properties: {},
      idDataConnector: 10,
      grpcMethodType: "ClientStreaming",
      methodName: "Client"
    },
    {
      id: 102,
      name: "server",
      properties: {},
      idDataConnector: 10,
      grpcMethodType: "ServerStreaming",
      methodName: "Server"
    },
    {
      id: 103,
      name: "bidi",
      properties: {},
      idDataConnector: 10,
      grpcMethodType: "BidirectionalStreaming",
      methodName: "Bidi"
    }
  ];
  const streamConfigs: InputStreamConfig[] = endpointConfigs.map((endpoint, index) => ({
    id: index + 1,
    name: endpoint.name,
    properties: {},
    type: "Input",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: index,
    yPos: 0,
    idEndpoint: endpoint.id,
    valueType: "timestamp"
  }));
  const tracing = new TestTracing();
  const environment = makeTestEnvironment(streamConfigs, {
    dataConnectors: [connector],
    endpoints: endpointConfigs,
    service: { grpcPort: 19203 },
    tracing
  });
  registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  const streams = streamConfigs.map((config) => {
    environment.serdeRegistry().registerStreamErrorType(config.id, errorSerdeType);
    const stream = makeInputStream<Timestamp, Timestamp, Error>(config, environment);
    const consumerConfig: StreamConfig = {
      ...config,
      id: config.id + 10,
      name: `${config.name}Consumer`,
      type: "Sink",
      idSource: config.id
    };
    const consumer = new RecordingConsumer(consumerConfig, environment);
    stream.setConsumer(consumer);
    return { stream, consumer };
  });
  const [service, clientMethod, serverMethod, bidiMethod] = descriptors();
  const clientHandler = new StreamingHandler(true);
  const serverHandler = new StreamingHandler(false);
  const bidiHandler = new StreamingHandler(false);
  const [clientStream, serverStream, bidiStream] = streams;
  assert.ok(clientStream);
  assert.ok(serverStream);
  assert.ok(bidiStream);
  makeGrpcClientStreamingEndpointConsumer(
    clientStream.stream,
    service,
    clientMethod,
    clientHandler
  );
  makeGrpcServerStreamingEndpointConsumer(
    serverStream.stream,
    service,
    serverMethod,
    serverHandler
  );
  makeGrpcBidiStreamingEndpointConsumer(bidiStream.stream, service, bidiMethod, bidiHandler);
  const source = environment.dataSourceById(10);
  assert.ok(source);
  await source.start(Context.background());
  const grpcClient = new Client("127.0.0.1:19203", credentials.createInsecure());
  const serialize = (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value));
  const deserialize = (bytes: Buffer) => fromBinary(TimestampSchema, bytes);
  const values = [1n, 2n, 3n].map((seconds) => create(TimestampSchema, { seconds, nanos: 0 }));
  const [firstValue] = values;
  assert.ok(firstValue);
  const metadata = new Metadata();
  metadata.set("x-trace", "1");
  try {
    const clientResponse = await within(
      "client streaming",
      new Promise<Timestamp>((resolve, reject) => {
        const call = grpcClient.makeClientStreamRequest(
          "/test.StreamingService/Client",
          serialize,
          deserialize,
          metadata,
          (error, value) => {
            if (error !== null) reject(error);
            else if (value === undefined) reject(new Error("client stream returned no value"));
            else resolve(value);
          }
        );
        for (const value of values) call.write(value);
        call.end();
      })
    );
    assert.equal(clientResponse.seconds, 3n);

    const serverResponses = await within(
      "server streaming",
      new Promise<Timestamp[]>((resolve, reject) => {
        const received: Timestamp[] = [];
        const call = grpcClient.makeServerStreamRequest(
          "/test.StreamingService/Server",
          serialize,
          deserialize,
          firstValue,
          metadata
        );
        call.on("data", (value: Timestamp) => received.push(value));
        call.on("error", reject);
        call.on("status", (status) => {
          if (status.code === grpcStatus.OK) resolve(received);
        });
      })
    );
    assert.deepEqual(
      serverResponses.map((value) => value.seconds),
      [1n]
    );

    const bidiResponses = await within(
      () => `bidi streaming ${JSON.stringify(bidiHandler.lifecycle)}`,
      new Promise<Timestamp[]>((resolve, reject) => {
        const received: Timestamp[] = [];
        const call = grpcClient.makeBidiStreamRequest(
          "/test.StreamingService/Bidi",
          serialize,
          deserialize,
          metadata
        );
        call.on("data", (value: Timestamp) => received.push(value));
        call.on("error", reject);
        call.on("status", (status) => {
          if (status.code === grpcStatus.OK) resolve(received);
        });
        for (const value of values) call.write(value);
        call.end();
      })
    );
    assert.deepEqual(
      bidiResponses.map((value) => value.seconds),
      [1n, 2n, 3n]
    );
    assert.deepEqual(clientStream.consumer.values, [1n, 2n, 3n]);
    assert.deepEqual(serverStream.consumer.values, [1n]);
    assert.deepEqual(bidiStream.consumer.values, [1n, 2n, 3n]);
    assert.deepEqual(clientHandler.lifecycle, [
      "begin",
      "consume",
      "consume",
      "consume",
      "eof",
      "end"
    ]);
    assert.deepEqual(serverHandler.lifecycle, ["begin", "consume", "eof", "end"]);
    assert.deepEqual(bidiHandler.lifecycle, [
      "begin",
      "consume",
      "consume",
      "consume",
      "eof",
      "end"
    ]);
    const spans = new Map<string, readonly string[]>();
    for (const span of tracing.spans()) {
      if (span.name !== "grpc.input") continue;
      const endpointAttribute = span.attributes.find(({ key }) => key === "endpoint");
      assert.ok(endpointAttribute?.type === "string");
      spans.set(
        endpointAttribute.value,
        span.events.map(({ name }) => name)
      );
    }
    assert.deepEqual(spans.get("client"), ["begin_request", "eof", "send"]);
    assert.deepEqual(spans.get("server"), ["begin_request", "send", "consume_message", "eof"]);
    assert.deepEqual(spans.get("bidi"), ["begin_request", "send", "send", "send", "eof"]);
  } finally {
    grpcClient.close();
    await source.stop(Context.background().bounded(100));
  }
});
