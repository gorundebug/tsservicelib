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
import {
  Server,
  ServerCredentials,
  type ServiceDefinition,
  type handleUnaryCall
} from "@grpc/grpc-js";

import {
  type EndpointHandler,
  type ResultContext,
  type Sender,
  makeGrpcNoStreamingEndpointConsumer
} from "@gorundebug/tsservicelib/datasink/grpc";
import { makeSinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  MessageContext,
  ServiceStream,
  errorSerdeType,
  type Completion,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig,
  type MessageContext as MessageContextType,
  type SinkStreamConfig,
  type SinkStreamContext,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { TestTracing } from "@gorundebug/tsservicelib/runtime/testtracing";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

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
  name: "sink",
  properties: {},
  type: "Sink",
  pipeline: "main",
  idService: 1,
  idSource: 1,
  idSources: [],
  xPos: 1,
  yPos: 0,
  idEndpoint: 100,
  valueType: "timestamp"
};
const resultConfig: StreamConfig = { ...sourceConfig, id: 3, name: "result", idSource: 2 };
const connector: GrpcDataConnectorConfig = {
  id: 10,
  name: "echo",
  properties: {},
  type: 2,
  implementation: "grpc/grpc-js",
  address: "127.0.0.1:19202",
  connectionsCount: 3
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
    typeName: "test.EchoSink",
    name: "EchoSink",
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => "test.EchoSink"
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
    toString: () => "test.EchoSink.Echo"
  } as unknown as DescMethod;
  service.methods.push(method);
  service.method["echo"] = method;
  return [service, method];
}

class RecordingStream extends ServiceStream implements TypedStreamConsumer<Timestamp> {
  public readonly values: Timestamp[] = [];
  public consume(_context: MessageContextType, value: Timestamp): void {
    this.values.push(value);
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
  public beginRequest(
    context: MessageContextType
  ): Promise<{ readonly context: MessageContextType; readonly state: undefined }> {
    return Promise.resolve({ context, state: undefined });
  }
  public consumeMessage(
    context: MessageContextType,
    _stream: SinkStreamContext<Timestamp, Timestamp, Error>,
    _state: undefined,
    value: Timestamp,
    sender: Sender<Timestamp>,
    _result: ResultContext
  ): Completion {
    void _result;
    return sender.send(context, value);
  }
  public handleResponse(
    context: MessageContextType,
    stream: SinkStreamContext<Timestamp, Timestamp, Error>,
    _state: undefined,
    response: Timestamp
  ): Promise<void> {
    return Promise.resolve(stream.collect(context, response));
  }
  public endRequest(): void {
    return;
  }
}

await test("gRPC unary sink sends a request and collects its response", async () => {
  const [service, method] = descriptors();
  const server = new Server();
  const definition: ServiceDefinition = {
    echo: {
      path: "/test.EchoSink/Echo",
      requestStream: false,
      responseStream: false,
      requestSerialize: (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      requestDeserialize: (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
      responseSerialize: (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      responseDeserialize: (bytes: Buffer) => fromBinary(TimestampSchema, bytes)
    }
  };
  const receivedStreamIds: string[] = [];
  let receivedSampling: string | undefined;
  let receivedBaggage: string | undefined;
  let receivedPrivateMetadata: string | undefined;
  let receivedDeadline: number | undefined;
  const peers = new Set<string>();
  const echo: handleUnaryCall<Timestamp, Timestamp> = (call, callback) => {
    peers.add(call.getPeer());
    const streamId = call.metadata.get("x-stream-id")[0]?.toString();
    if (streamId !== undefined) receivedStreamIds.push(streamId);
    receivedSampling = call.metadata.get("x-trace")[0]?.toString();
    receivedBaggage = call.metadata.get("baggage")[0]?.toString();
    receivedPrivateMetadata = call.metadata.get("x-private")[0]?.toString();
    const deadline = call.getDeadline();
    receivedDeadline = deadline instanceof Date ? deadline.getTime() : deadline;
    callback(null, call.request);
  };
  server.addService(definition, { echo });
  await new Promise<void>((resolve, reject) => {
    server.bindAsync("127.0.0.1:19202", ServerCredentials.createInsecure(), (error) => {
      if (error === null) resolve();
      else reject(error);
    });
  });
  const tracing = new TestTracing();
  const environment = makeTestEnvironment([sourceConfig, sinkConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint],
    tracing
  });
  const timestampType = registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(timestampType));
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  const sink = makeSinkStreamWithResult(sinkConfig, source);
  const results = new RecordingStream(resultConfig, environment);
  sink.setConsumer(results);
  makeGrpcNoStreamingEndpointConsumer(sink, service, method, new Handler());
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink);
  await dataSink.start(Context.background());
  try {
    for (let index = 0; index < 6; index += 1) {
      await source.emit(
        new MessageContext()
          .withMetadata(
            new Map([
              ["x-stream-id", "transport-stream"],
              ["x-trace", "1"],
              ["baggage", "tenant=alpha"],
              ["x-private", "must-not-cross-runtime-boundary"]
            ])
          )
          .bounded(5_000),
        create(TimestampSchema, { seconds: 9n, nanos: 3 })
      );
    }
    const response = results.values[0];
    assert.ok(response);
    assert.equal(response.seconds, 9n);
    assert.equal(response.nanos, 3);
    assert.equal(receivedStreamIds.length, 6);
    assert.equal(new Set(receivedStreamIds).size, 6);
    assert.ok(receivedStreamIds.every((streamId) => streamId !== "transport-stream"));
    assert.equal(receivedSampling, "1");
    assert.equal(receivedBaggage, "tenant=alpha");
    assert.equal(receivedPrivateMetadata, undefined);
    assert.ok((receivedDeadline ?? 0) > Date.now());
    assert.ok((receivedDeadline ?? Infinity) <= Date.now() + 5_000);
    assert.equal(peers.size, 3);
    const spans = tracing.spans().filter(({ name }) => name === "grpc.output");
    assert.equal(spans.length, 6);
    for (const span of spans) {
      assert.deepEqual(
        span.events.map(({ name }) => name),
        ["begin_request", "consume_message", "grpc_call", "handle_response"]
      );
    }
  } finally {
    await dataSink.stop(Context.background());
    await new Promise<void>((resolve, reject) => {
      server.tryShutdown((error) => {
        if (error === undefined) resolve();
        else reject(error);
      });
    });
  }
});
