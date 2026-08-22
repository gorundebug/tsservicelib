import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
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
  type handleBidiStreamingCall,
  type handleClientStreamingCall,
  type handleServerStreamingCall
} from "@grpc/grpc-js";

import {
  makeGrpcBidiStreamingEndpointConsumer,
  makeGrpcClientStreamingEndpointConsumer,
  makeGrpcServerStreamingEndpointConsumer,
  type EndpointHandler,
  type ResultContext,
  type Sender
} from "@gorundebug/tsservicelib/datasink/grpc";
import { makeSinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  MessageContext,
  ServiceStream,
  errorSerdeType,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig,
  type MessageContext as MessageContextType,
  type SinkStreamConfig,
  type SinkStreamContext,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

interface HandlerState {
  readonly mode: "client" | "server" | "bidi";
}

class Handler implements EndpointHandler<
  HandlerState,
  Timestamp,
  Timestamp,
  Timestamp,
  Timestamp,
  Error
> {
  readonly #mode: HandlerState["mode"];
  readonly lifecycle: string[] = [];

  public constructor(mode: HandlerState["mode"]) {
    this.#mode = mode;
  }

  public beginRequest(
    context: MessageContextType
  ): Promise<{ readonly context: MessageContextType; readonly state: HandlerState }> {
    this.lifecycle.push("begin");
    return Promise.resolve({ context, state: { mode: this.#mode } });
  }

  public async consumeMessage(
    context: MessageContextType,
    _stream: SinkStreamContext<Timestamp, Timestamp, Error>,
    state: HandlerState,
    value: Timestamp,
    sender: Sender<Timestamp>,
    result: ResultContext
  ): Promise<void> {
    this.lifecycle.push(`consume:${String(value.seconds)}`);
    await sender.send(context, value);
    if (state.mode !== "server" && value.seconds === 3n) result.done();
  }

  public handleResponse(
    context: MessageContextType,
    stream: SinkStreamContext<Timestamp, Timestamp, Error>,
    _state: HandlerState,
    response: Timestamp
  ): Promise<void> {
    this.lifecycle.push(`response:${String(response.seconds)}`);
    return Promise.resolve(stream.collect(context, response));
  }

  public endRequest(
    _context: MessageContextType,
    _stream: SinkStreamContext<Timestamp, Timestamp, Error>,
    error: Error | undefined
  ): void {
    this.lifecycle.push(error === undefined ? "end" : `end:${error.message}`);
  }
}

class RecordingStream extends ServiceStream implements TypedStreamConsumer<Timestamp> {
  readonly values: bigint[] = [];

  public consume(_context: MessageContextType, value: Timestamp): void {
    this.values.push(value.seconds);
  }
}

function descriptors(): readonly [DescService, DescMethod, DescMethod, DescMethod] {
  const service = {
    kind: "service",
    typeName: "test.StreamingSink",
    name: "StreamingSink",
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => "test.StreamingSink"
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
      toString: () => `test.StreamingSink.${name}`
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

async function waitUntil(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("condition did not become true");
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

await test("gRPC streaming sinks preserve stream identity and canonical lifecycle", async () => {
  const [service, clientMethod, serverMethod, bidiMethod] = descriptors();
  const serialize = (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value));
  const deserialize = (bytes: Buffer) => fromBinary(TimestampSchema, bytes);
  const definition: ServiceDefinition = Object.fromEntries(
    service.methods.map((method) => [
      method.localName,
      {
        path: `/${service.typeName}/${method.name}`,
        requestStream: method.methodKind !== "server_streaming",
        responseStream: method.methodKind !== "client_streaming",
        requestSerialize: serialize,
        requestDeserialize: deserialize,
        responseSerialize: serialize,
        responseDeserialize: deserialize
      }
    ])
  );
  const client: handleClientStreamingCall<Timestamp, Timestamp> = (call, callback) => {
    let last = create(TimestampSchema);
    call.on("data", (value: Timestamp) => {
      last = value;
    });
    call.on("end", () => {
      callback(null, last);
    });
  };
  const server: handleServerStreamingCall<Timestamp, Timestamp> = (call) => {
    call.write(call.request);
    call.write(call.request);
    call.end();
  };
  const bidi: handleBidiStreamingCall<Timestamp, Timestamp> = (call) => {
    call.on("data", (value: Timestamp) => call.write(value));
    call.on("end", () => call.end());
  };
  const grpcServer = new Server();
  grpcServer.addService(definition, { client, server, bidi });
  await new Promise<void>((resolve, reject) => {
    grpcServer.bindAsync("127.0.0.1:19204", ServerCredentials.createInsecure(), (error) => {
      if (error === null) resolve();
      else reject(error);
    });
  });

  const modes = ["client", "server", "bidi"] as const;
  const sources: StreamConfig[] = modes.map((mode, index) => ({
    id: index + 1,
    name: `${mode}Source`,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: 0,
    idSources: [],
    xPos: index,
    yPos: 0
  }));
  const sinks: SinkStreamConfig[] = modes.map((mode, index) => ({
    id: index + 11,
    name: `${mode}Sink`,
    properties: {},
    type: "Sink",
    pipeline: "main",
    idService: 1,
    idSource: index + 1,
    idSources: [],
    xPos: index,
    yPos: 1,
    idEndpoint: index + 101,
    valueType: "timestamp"
  }));
  const results: StreamConfig[] = modes.map((mode, index) => ({
    id: index + 21,
    name: `${mode}Result`,
    properties: {},
    type: "Map",
    pipeline: "main",
    idService: 1,
    idSource: index + 11,
    idSources: [],
    xPos: index,
    yPos: 2
  }));
  const connector: GrpcDataConnectorConfig = {
    id: 10,
    name: "streaming",
    properties: {},
    type: 2,
    implementation: "grpc/grpc-js",
    address: "127.0.0.1:19204",
    connectionsCount: 1
  };
  const endpoints: GrpcEndpointConfig[] = [
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
  const environment = makeTestEnvironment([...sources, ...sinks, ...results], {
    dataConnectors: [connector],
    endpoints
  });
  const timestampType = registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  const handlers = modes.map((mode) => new Handler(mode));
  const recording: RecordingStream[] = [];
  const roots = sources.map((config, index) => {
    const source = new ConsumedStream(config, environment, environment.serde(timestampType));
    const sinkConfig = sinks[index];
    const resultConfig = results[index];
    assert.ok(sinkConfig);
    assert.ok(resultConfig);
    environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
    const sink = makeSinkStreamWithResult<Timestamp, Timestamp, Error>(sinkConfig, source);
    const result = new RecordingStream(resultConfig, environment);
    sink.setConsumer(result);
    recording.push(result);
    return { source, sink };
  });
  const [clientRoot, serverRoot, bidiRoot] = roots;
  const [clientHandler, serverHandler, bidiHandler] = handlers;
  assert.ok(clientRoot && serverRoot && bidiRoot);
  assert.ok(clientHandler && serverHandler && bidiHandler);
  makeGrpcClientStreamingEndpointConsumer(clientRoot.sink, service, clientMethod, clientHandler);
  makeGrpcServerStreamingEndpointConsumer(serverRoot.sink, service, serverMethod, serverHandler);
  makeGrpcBidiStreamingEndpointConsumer(bidiRoot.sink, service, bidiMethod, bidiHandler);
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink);
  await dataSink.start(Context.background());
  const values = [1n, 2n, 3n].map((seconds) => create(TimestampSchema, { seconds }));
  const [firstValue] = values;
  assert.ok(firstValue);
  const streamContext = new MessageContext().withStreamId("same-stream").bounded(5_000);
  const baselineAbortListeners = getEventListeners(streamContext.signal(), "abort").length;
  try {
    for (const value of values) await clientRoot.source.emit(streamContext, value);
    await serverRoot.source.emit(streamContext, firstValue);
    for (const value of values) await bidiRoot.source.emit(streamContext, value);
    await waitUntil(() => recording.every((result) => result.values.length > 0));
    assert.deepEqual(recording[0]?.values, [3n]);
    assert.deepEqual(recording[1]?.values, [1n, 1n]);
    assert.deepEqual(recording[2]?.values, [1n, 2n, 3n]);
    await waitUntil(() => handlers.every((handler) => handler.lifecycle.at(-1) === "end"));
    assert.equal(clientHandler.lifecycle.filter((event) => event === "begin").length, 1);
    assert.equal(bidiHandler.lifecycle.filter((event) => event === "begin").length, 1);
    assert.equal(getEventListeners(streamContext.signal(), "abort").length, baselineAbortListeners);
  } finally {
    await dataSink.stop(Context.background());
    await new Promise<void>((resolve) => {
      grpcServer.tryShutdown(() => {
        resolve();
      });
    });
  }
});
