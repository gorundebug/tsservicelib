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
  type handleServerStreamingCall,
  type handleUnaryCall
} from "@grpc/grpc-js";

import {
  GrpcJsDataSink,
  makeGrpcNoStreamingEndpointConsumer,
  makeGrpcServerStreamingEndpointConsumer,
  type EndpointHandler
} from "@gorundebug/tsservicelib/datasink/grpc";
import { makeSinkStreamWithResult } from "@gorundebug/tsservicelib/operators";
import {
  ConsumedStream,
  Context,
  MessageContext,
  errorSerdeType,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig,
  type SinkStreamConfig,
  type StreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

type Mode = "unary" | "server";
type Handler = EndpointHandler<undefined, Timestamp, Timestamp, Timestamp, Timestamp, Error>;

function gate(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error("gRPC lifecycle test timed out"));
        }, 2_000);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function handler(overrides: Partial<Handler>): Handler {
  return {
    beginRequest: (context) => ({ context, state: undefined }),
    consumeMessage: (context, _stream, _state, value, sender, result) => {
      // Done is deliberately a no-op for these request modes, as in Go.
      result.done();
      return sender.send(context, value);
    },
    handleResponse: () => undefined,
    endRequest: () => undefined,
    ...overrides
  };
}

async function fixture(mode: Mode, business: Handler, holdResponse = false) {
  const service = {
    kind: "service",
    typeName: "test.SingleRequestLifecycle",
    name: "SingleRequestLifecycle",
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => "test.SingleRequestLifecycle"
  } as unknown as DescService;
  const method = {
    kind: "rpc",
    name: "Exchange",
    localName: "exchange",
    parent: service,
    methodKind: mode === "unary" ? "unary" : "server_streaming",
    input: TimestampSchema,
    output: TimestampSchema,
    deprecated: false,
    idempotency: undefined,
    proto: {},
    toString: () => "test.SingleRequestLifecycle.Exchange"
  } as unknown as DescMethod;
  service.methods.push(method);
  service.method["exchange"] = method;
  const definition: ServiceDefinition = {
    exchange: {
      path: "/test.SingleRequestLifecycle/Exchange",
      requestStream: false,
      responseStream: mode === "server",
      requestSerialize: (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      requestDeserialize: (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
      responseSerialize: (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      responseDeserialize: (bytes: Buffer) => fromBinary(TimestampSchema, bytes)
    }
  };
  const arrived = gate();
  const cancelled = gate();
  const networkIds: string[] = [];
  const server = new Server();
  const unary: handleUnaryCall<Timestamp, Timestamp> = (call, callback) => {
    networkIds.push(String(call.metadata.get("x-stream-id")[0]));
    call.on("cancelled", cancelled.resolve);
    arrived.resolve();
    if (!holdResponse) callback(null, call.request);
  };
  const streaming: handleServerStreamingCall<Timestamp, Timestamp> = (call) => {
    networkIds.push(String(call.metadata.get("x-stream-id")[0]));
    call.on("cancelled", cancelled.resolve);
    arrived.resolve();
    if (!holdResponse) {
      call.write(call.request);
      call.write(create(TimestampSchema, { seconds: call.request.seconds + 10n }));
      call.end();
    }
  };
  server.addService(definition, { exchange: mode === "unary" ? unary : streaming });
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, value) => {
      if (error === null) resolve(value);
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
  const secondSource = { ...sourceConfig, id: 4, name: "secondSource" };
  const sinkConfig: SinkStreamConfig = {
    ...sourceConfig,
    id: 2,
    name: "sink",
    type: "Sink",
    idSource: 1,
    idEndpoint: 100,
    valueType: "timestamp"
  };
  const secondSink = { ...sinkConfig, id: 5, name: "secondSink", idSource: 4 };
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
    grpcMethodType: mode === "unary" ? "NoStreaming" : "ServerStreaming",
    methodName: "Exchange"
  };
  const environment = makeTestEnvironment([sourceConfig, sinkConfig, secondSource, secondSink], {
    dataConnectors: [connector],
    endpoints: [endpoint]
  });
  const timestamp = registerTestSerdeType<Timestamp>(
    environment,
    "timestamp",
    (value): value is Timestamp => typeof value === "object" && value !== null
  );
  const streamPairs: readonly (readonly [StreamConfig, SinkStreamConfig])[] = [
    [sourceConfig, sinkConfig],
    [secondSource, secondSink]
  ];
  const consumers = streamPairs.map(([sourceOptions, sinkOptions]) => {
    const source = new ConsumedStream(sourceOptions, environment, environment.serde(timestamp));
    environment.serdeRegistry().registerStreamErrorType(sinkOptions.id, errorSerdeType);
    const sink = makeSinkStreamWithResult<Timestamp, Timestamp, Error>(sinkOptions, source);
    return mode === "unary"
      ? makeGrpcNoStreamingEndpointConsumer(sink, service, method, business)
      : makeGrpcServerStreamingEndpointConsumer(sink, service, method, business);
  });
  const dataSink = environment.dataSinkById(10);
  assert.ok(dataSink instanceof GrpcJsDataSink);
  await dataSink.start(Context.background());
  const cancellation = new AbortController();
  const context = new MessageContext(cancellation.signal).withStreamId("parent");
  return {
    dataSink,
    context,
    networkIds,
    arrived: arrived.promise,
    cancelled: cancelled.promise,
    abort: () => {
      cancellation.abort(new Error("test cancellation"));
    },
    emit(value: bigint, consumerIndex = 0, inputContext = context): Promise<void> {
      const consumer = consumers[consumerIndex];
      assert.ok(consumer);
      return Promise.resolve(
        consumer.consume(inputContext, create(TimestampSchema, { seconds: value }))
      );
    },
    async close(): Promise<void> {
      cancellation.abort(new Error("fixture closed"));
      await within(dataSink.stop(Context.background().bounded(500)));
      server.forceShutdown();
    }
  };
}

for (const mode of ["unary", "server"] as const) {
  await test(`${mode} sink isolates concurrent calls and shared endpoint consumers`, async () => {
    const responseEntered = gate();
    const responseRelease = gate();
    const endEntered = gate();
    const endRelease = gate();
    const events: string[] = [];
    const parentIds: (string | undefined)[] = [];
    const app = await fixture(
      mode,
      handler({
        handleResponse: async (context, _stream, _state, value) => {
          parentIds.push(context.streamId());
          events.push(`response:${String(value.seconds)}`);
          if (value.seconds === 1n) {
            responseEntered.resolve();
            await responseRelease.promise;
          }
        },
        endRequest: async (context, _stream, error) => {
          assert.equal(error, undefined);
          parentIds.push(context.streamId());
          events.push("end");
          endEntered.resolve();
          await endRelease.promise;
        }
      })
    );
    const calls: Promise<void>[] = [];
    try {
      calls.push(app.emit(1n));
      await within(responseEntered.promise);
      assert.deepEqual(events, ["response:1"]);
      // Another sink on the same endpoint can progress while the first handler waits.
      calls.push(app.emit(2n, 1));
      await within(endEntered.promise);
      assert.ok(events.includes("response:2"));
      assert.equal(app.networkIds.length, 2);
      assert.equal(new Set(app.networkIds).size, 2);
      assert.ok(app.networkIds.every((id) => id !== "parent" && id !== "undefined"));
      // Neither a completed transport nor connector shutdown destroys active handlers.
      await within(app.dataSink.stop(Context.background().bounded(100)));
      responseRelease.resolve();
      endRelease.resolve();
      await within(Promise.all(calls));
      assert.equal(events.filter((event) => event === "end").length, 2);
      assert.ok(parentIds.every((id) => id === "parent"));
      if (mode === "server") {
        assert.ok(events.indexOf("response:11") > events.indexOf("response:1"));
        assert.ok(events.indexOf("response:12") > events.indexOf("response:2"));
      }
    } finally {
      responseRelease.resolve();
      endRelease.resolve();
      await app.close();
      await Promise.allSettled(calls);
    }
  });

  for (const failure of ["consume", "response", "end"] as const) {
    await test(`${mode} sink finalizes once after ${failure} failure`, async () => {
      const expected = new Error(`${failure} failure`);
      let ended = 0;
      let received = 0;
      let finalError: Error | undefined;
      const app = await fixture(
        mode,
        handler({
          consumeMessage: (context, _stream, _state, value, sender) => {
            if (failure === "consume") throw expected;
            return sender.send(context, value);
          },
          handleResponse: () => {
            received += 1;
            if (failure === "response") throw expected;
          },
          endRequest: (_context, _stream, error) => {
            ended += 1;
            finalError = error;
            if (failure === "end") throw expected;
          }
        })
      );
      try {
        await within(app.emit(1n));
        assert.equal(ended, 1);
        assert.equal(finalError, failure === "end" ? undefined : expected);
        assert.equal(
          received,
          failure === "consume" ? 0 : failure === "response" || mode === "unary" ? 1 : 2
        );
        assert.equal(app.networkIds.length, failure === "consume" ? 0 : 1);
      } finally {
        await app.close();
      }
    });
  }

  for (const cancellation of ["abort", "deadline", "abort after connector stop"] as const) {
    await test(`${mode} sink finalizes a pending transport on ${cancellation}`, async () => {
      let ended = 0;
      let received = 0;
      let finalError: Error | undefined;
      const app = await fixture(
        mode,
        handler({
          handleResponse: () => {
            received += 1;
          },
          endRequest: (_context, _stream, error) => {
            ended += 1;
            finalError = error;
          }
        }),
        true
      );
      const call = app.emit(
        1n,
        0,
        cancellation === "deadline" ? app.context.bounded(500) : app.context
      );
      try {
        await within(app.arrived);
        if (cancellation === "abort") app.abort();
        if (cancellation === "abort after connector stop") {
          await within(app.dataSink.stop(Context.background().bounded(100)));
          // Go stops these consumers without cancelling an accepted RPC.
          // The request context remains responsible for its cancellation.
          assert.equal(ended, 0);
          assert.equal(received, 0);
          app.abort();
        }
        await within(call);
        assert.equal(ended, 1);
        assert.equal(received, 0);
        assert.ok(finalError instanceof Error);
        await within(app.cancelled);
      } finally {
        await app.close();
        await Promise.allSettled([call]);
      }
    });
  }
}
