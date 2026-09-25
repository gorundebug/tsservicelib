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
  type handleBidiStreamingCall,
  type handleClientStreamingCall
} from "@grpc/grpc-js";

import {
  GrpcJsDataSink,
  makeGrpcBidiStreamingEndpointConsumer,
  makeGrpcClientStreamingEndpointConsumer,
  type EndpointHandler
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
  type SinkStreamConfig,
  type StreamConfig,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

type Mode = "client" | "bidi";
type Handler = EndpointHandler<undefined, Timestamp, Timestamp, Timestamp, Timestamp, Error>;

function makeHandler(overrides: Partial<Handler>): Handler {
  return {
    beginRequest: (context) => ({ context, state: undefined }),
    consumeMessage: (context, _stream, _state, value, sender) => sender.send(context, value),
    handleResponse: (context, stream, _state, response) => stream.collect(context, response),
    endRequest: () => undefined,
    ...overrides
  };
}

function barrier(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Timed out waiting for ${label}`));
        }, 2_000);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

class RecordingStream extends ServiceStream implements TypedStreamConsumer<Timestamp> {
  public readonly values: bigint[] = [];

  public consume(_context: MessageContext, value: Timestamp): void {
    this.values.push(value.seconds);
  }
}

async function fixture(mode: Mode, handler: Handler) {
  const service = {
    kind: "service",
    typeName: "test.Lifecycle",
    name: "Lifecycle",
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => "test.Lifecycle"
  } as unknown as DescService;
  const method = {
    kind: "rpc",
    name: "Exchange",
    localName: "exchange",
    parent: service,
    methodKind: mode === "client" ? "client_streaming" : "bidi_streaming",
    input: TimestampSchema,
    output: TimestampSchema,
    deprecated: false,
    idempotency: undefined,
    proto: {},
    toString: () => "test.Lifecycle.Exchange"
  } as unknown as DescMethod;
  service.methods.push(method);
  service.method["exchange"] = method;

  const definition: ServiceDefinition = {
    exchange: {
      path: "/test.Lifecycle/Exchange",
      requestStream: true,
      responseStream: mode === "bidi",
      requestSerialize: (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      requestDeserialize: (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
      responseSerialize: (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      responseDeserialize: (bytes: Buffer) => fromBinary(TimestampSchema, bytes)
    }
  };
  const halfClosed = barrier();
  const server = new Server();
  const client: handleClientStreamingCall<Timestamp, Timestamp> = (call, callback) => {
    let last = create(TimestampSchema);
    call.on("data", (value: Timestamp) => {
      last = value;
    });
    call.on("end", () => {
      halfClosed.resolve();
      callback(null, last);
    });
  };
  const bidi: handleBidiStreamingCall<Timestamp, Timestamp> = (call) => {
    call.on("data", (value: Timestamp) => {
      call.write(value);
    });
    call.on("end", () => {
      halfClosed.resolve();
      call.end();
    });
  };
  server.addService(definition, { exchange: mode === "client" ? client : bidi });
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
      if (error === null) resolve(port);
      else reject(error);
    });
  });
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
    ...sourceConfig,
    id: 2,
    name: "sink",
    type: "Sink",
    idSource: 1,
    idEndpoint: 100,
    valueType: "timestamp"
  };
  const resultConfig: StreamConfig = { ...sourceConfig, id: 3, name: "result", idSource: 2 };
  const connector: GrpcDataConnectorConfig = {
    id: 10,
    name: "lifecycle",
    properties: {},
    type: 2,
    implementation: "grpc/grpc-js",
    address: `127.0.0.1:${String(port)}`,
    connectionsCount: 1
  };
  const endpoint: GrpcEndpointConfig = {
    id: 100,
    name: "exchange",
    properties: {},
    idDataConnector: 10,
    grpcMethodType: mode === "client" ? "ClientStreaming" : "BidirectionalStreaming",
    methodName: "Exchange"
  };
  const environment = makeTestEnvironment([sourceConfig, sinkConfig, resultConfig], {
    dataConnectors: [connector],
    endpoints: [endpoint]
  });
  const timestampType = registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  const source = new ConsumedStream(sourceConfig, environment, environment.serde(timestampType));
  environment.serdeRegistry().registerStreamErrorType(sinkConfig.id, errorSerdeType);
  const sink = makeSinkStreamWithResult<Timestamp, Timestamp, Error>(sinkConfig, source);
  const results = new RecordingStream(resultConfig, environment);
  sink.setConsumer(results);
  if (mode === "client") {
    makeGrpcClientStreamingEndpointConsumer(sink, service, method, handler);
  } else {
    makeGrpcBidiStreamingEndpointConsumer(sink, service, method, handler);
  }
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink instanceof GrpcJsDataSink);
  await dataSink.start(Context.background());
  const cancellation = new AbortController();
  const context = new MessageContext(cancellation.signal)
    .withStreamId("same-stream")
    .bounded(10_000);
  return {
    halfClosed: halfClosed.promise,
    results,
    dataSink,
    abort: () => {
      cancellation.abort(new Error("request cancelled"));
    },
    emit: (value: bigint) =>
      Promise.resolve(source.emit(context, create(TimestampSchema, { seconds: value }))),
    async close(): Promise<void> {
      cancellation.abort(new Error("fixture closed"));
      server.forceShutdown();
      await within(dataSink.stop(Context.background().bounded(1_000)), "sink stop");
    }
  };
}

for (const mode of ["client", "bidi"] as const) {
  await test(`gRPC ${mode} Done half-closes before ConsumeMessage finishes`, async () => {
    const entered = barrier();
    const release = barrier();
    const ended = barrier();
    const f = await fixture(
      mode,
      makeHandler({
        async consumeMessage(context, _stream, _state, value, sender, result) {
          await sender.send(context, value);
          result.done();
          entered.resolve();
          await release.promise;
        },
        endRequest: () => {
          ended.resolve();
        }
      })
    );
    const active = f.emit(1n);
    try {
      await within(entered.promise, "ConsumeMessage entry");
      await within(f.halfClosed, "transport half-close before releasing ConsumeMessage");
      if (mode === "client") assert.deepEqual(f.results.values, []);
      release.resolve();
      await within(active, "ConsumeMessage return");
      await within(ended.promise, "EndRequest");
      assert.deepEqual(f.results.values, [1n]);
    } finally {
      release.resolve();
      await active;
      await f.close();
    }
  });

  await test(`gRPC ${mode} does not serialize independent ConsumeMessage callbacks`, async () => {
    const first = barrier();
    const second = barrier();
    const release = barrier();
    const ended = barrier();
    const f = await fixture(
      mode,
      makeHandler({
        async consumeMessage(context, _stream, _state, value, sender, result) {
          await sender.send(context, value);
          if (value.seconds === 1n) first.resolve();
          else second.resolve();
          await release.promise;
          if (value.seconds === 2n) result.done();
        },
        endRequest: () => {
          ended.resolve();
        }
      })
    );
    const firstCall = f.emit(1n);
    let secondCall: Promise<void> | undefined;
    try {
      await within(first.promise, "first callback");
      secondCall = f.emit(2n);
      await within(second.promise, "second callback while first is suspended");
      release.resolve();
      await within(Promise.all([firstCall, secondCall]), "both callbacks");
      await within(ended.promise, "EndRequest");
    } finally {
      release.resolve();
      await firstCall;
      await secondCall;
      await f.close();
    }
  });

  await test(`gRPC ${mode} reserves streamId through EndRequest`, async () => {
    let begins = 0;
    const ending = barrier();
    const release = barrier();
    const f = await fixture(
      mode,
      makeHandler({
        beginRequest(context) {
          begins += 1;
          return { context, state: undefined };
        },
        async consumeMessage(context, _stream, _state, value, sender, result) {
          await sender.send(context, value);
          result.done();
        },
        async endRequest() {
          ending.resolve();
          await release.promise;
        }
      })
    );
    try {
      await f.emit(1n);
      await within(ending.promise, "EndRequest entry");
      await within(f.emit(2n), "same-ID rejection during EndRequest");
      assert.equal(begins, 1, "a finalizing stream must not open a second RPC");
    } finally {
      release.resolve();
      await f.close();
    }
  });

  await test(`gRPC ${mode} cancellation drains active ConsumeMessage before EndRequest`, async () => {
    const entered = barrier();
    const release = barrier();
    const ended = barrier();
    let endCalled = false;
    const f = await fixture(
      mode,
      makeHandler({
        async consumeMessage(context, _stream, _state, value, sender) {
          await sender.send(context, value);
          entered.resolve();
          await release.promise;
        },
        endRequest() {
          endCalled = true;
          ended.resolve();
        }
      })
    );
    const active = f.emit(1n);
    try {
      await within(entered.promise, "active ConsumeMessage");
      f.abort();
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
      assert.equal(endCalled, false, "cancellation must not destroy active handler state");
      release.resolve();
      await within(active, "ConsumeMessage return");
      await within(ended.promise, "EndRequest after callback drain");
    } finally {
      release.resolve();
      await active;
      await f.close();
    }
  });

  await test(`gRPC ${mode} failed open rejects reentry during EndRequest`, async (t) => {
    let begins = 0;
    let sends = 0;
    const ended = barrier();
    const f = await fixture(
      mode,
      makeHandler({
        beginRequest(context) {
          begins += 1;
          return { context, state: undefined };
        },
        consumeMessage() {
          sends += 1;
        },
        async endRequest() {
          await f.emit(2n);
          ended.resolve();
        }
      })
    );
    t.mock.method(f.dataSink, mode === "client" ? "clientStream" : "bidiStream", () => {
      throw new Error("injected RPC open failure");
    });
    try {
      await within(f.emit(1n), "failed RPC open");
      await within(ended.promise, "reentrant EndRequest");
      assert.equal(begins, 1);
      assert.equal(sends, 0);
    } finally {
      await f.close();
    }
  });

  await test(`gRPC ${mode} stop respects its deadline while ConsumeMessage is suspended`, async () => {
    const entered = barrier();
    const release = barrier();
    const ended = barrier();
    let endCalled = false;
    const f = await fixture(
      mode,
      makeHandler({
        async consumeMessage(context, _stream, _state, value, sender, result) {
          await sender.send(context, value);
          entered.resolve();
          await release.promise;
          result.done();
        },
        endRequest() {
          endCalled = true;
          ended.resolve();
        }
      })
    );
    const active = f.emit(1n);
    let stopping: Promise<void> | undefined;
    try {
      await within(entered.promise, "active handler before stop");
      stopping = f.dataSink.stop(Context.background().bounded(20));
      await within(stopping, "stop deadline without releasing the handler");
      assert.equal(endCalled, false, "stop must not finalize an active business callback");
      release.resolve();
      await within(active, "handler completion after stop");
      await within(ended.promise, "deferred EndRequest after stop");
    } finally {
      release.resolve();
      await active;
      f.abort();
      await stopping;
      await f.close();
    }
  });
}

await test("gRPC bidi cancellation drains active HandleResponse before EndRequest", async () => {
  const entered = barrier();
  const release = barrier();
  const ended = barrier();
  let endCalled = false;
  const f = await fixture(
    "bidi",
    makeHandler({
      async handleResponse() {
        entered.resolve();
        await release.promise;
      },
      endRequest() {
        endCalled = true;
        ended.resolve();
      }
    })
  );
  try {
    await f.emit(1n);
    await within(entered.promise, "active HandleResponse");
    f.abort();
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    assert.equal(endCalled, false, "EndRequest must wait for the in-flight response handler");
    release.resolve();
    await within(ended.promise, "EndRequest after response drain");
  } finally {
    release.resolve();
    await f.close();
  }
});
