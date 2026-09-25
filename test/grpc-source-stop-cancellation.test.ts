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
import { Client, credentials } from "@grpc/grpc-js";
import {
  makeGrpcNoStreamingEndpointConsumer,
  type EndpointHandler
} from "@gorundebug/tsservicelib/datasource/grpc";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  Context,
  errorSerdeType,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig,
  type InputStreamConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error("gRPC source stop ignored cancellation"));
        }, 700);
      })
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

for (const preCancelled of [false, true]) {
  await test(`gRPC source stop observes cancellation without deadline, preCancelled=${String(preCancelled)}`, async () => {
    const config: InputStreamConfig = {
      id: 1,
      name: "input",
      type: "Input",
      properties: {},
      pipeline: "main",
      idService: 1,
      idSource: 0,
      idSources: [],
      xPos: 0,
      yPos: 0,
      idEndpoint: 100,
      valueType: "timestamp"
    };
    const connector: GrpcDataConnectorConfig = {
      id: 10,
      name: "grpc",
      type: 2,
      properties: {},
      implementation: "grpc/grpc-js",
      connectionsCount: 1
    };
    const endpoint: GrpcEndpointConfig = {
      id: 100,
      name: "rpc",
      properties: {},
      idDataConnector: 10,
      grpcMethodType: "NoStreaming",
      methodName: "Call"
    };
    const environment = makeTestEnvironment([config], {
      dataConnectors: [connector],
      endpoints: [endpoint],
      service: { grpcPort: 19249 }
    });
    registerTestSerdeType<Timestamp>(
      environment,
      "timestamp",
      (value): value is Timestamp => typeof value === "object" && value !== null
    );
    environment.serdeRegistry().registerStreamErrorType(1, errorSerdeType);
    const input = makeInputStream<Timestamp, Timestamp, Error>(config, environment);
    let markEntered = (): void => undefined;
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    let cancelled = false;
    const handler: EndpointHandler<undefined, Timestamp, Timestamp, Timestamp, Timestamp, Error> = {
      beginRequest(context) {
        return { context, state: undefined };
      },
      async consumeMessage(context) {
        markEntered();
        await new Promise<void>((resolve) => {
          const done = (): void => {
            cancelled = true;
            resolve();
          };
          if (context.cancelled()) done();
          else context.signal().addEventListener("abort", done, { once: true });
        });
      },
      getMessageId() {
        return "message";
      },
      eof() {},
      endRequest() {}
    };
    const service = {
      kind: "service",
      name: "Stop",
      typeName: "test.Stop",
      methods: [],
      method: {},
      file: TimestampSchema.file
    } as unknown as DescService;
    const method = {
      kind: "rpc",
      name: "Call",
      localName: "call",
      parent: service,
      methodKind: "unary",
      input: TimestampSchema,
      output: TimestampSchema
    } as unknown as DescMethod;
    service.methods.push(method);
    service.method["call"] = method;
    makeGrpcNoStreamingEndpointConsumer(input, service, method, handler);
    const source = environment.dataSourceById(10);
    assert.ok(source);
    await source.start(Context.background());
    const client = new Client("127.0.0.1:19249", credentials.createInsecure());
    const request = client.makeUnaryRequest(
      "/test.Stop/Call",
      (value: Timestamp) => Buffer.from(toBinary(TimestampSchema, value)),
      (bytes: Buffer) => fromBinary(TimestampSchema, bytes),
      create(TimestampSchema),
      () => undefined
    );
    let stopping: Promise<void> | undefined;
    try {
      await within(entered);
      const controller = new AbortController();
      if (preCancelled) controller.abort(new Error("stop now"));
      stopping = source.stop(Context.background().withExternalCancellation(controller.signal));
      controller.abort(new Error("stop now"));
      await within(stopping);
      assert.equal(cancelled, true, "accepted call did not receive cancellation");
    } finally {
      request.cancel();
      client.close();
      await within(stopping ?? source.stop(Context.background().bounded(100)));
    }
  });
}
