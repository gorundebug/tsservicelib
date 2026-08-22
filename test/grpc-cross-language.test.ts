import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";
import { test } from "node:test";

import type { DescMethod } from "@bufbuild/protobuf";

import {
  makeGrpcBidiStreamingEndpointConsumer,
  makeGrpcClientStreamingEndpointConsumer,
  makeGrpcNoStreamingEndpointConsumer,
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
  type StreamConfig,
  type StreamContext,
  type TypedStreamConsumer
} from "@gorundebug/tsservicelib/runtime";
import { InteropService, type Echo } from "./generated/interop_pb.js";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

const execFileAsync = promisify(execFile);

interface HandlerState {
  last: Echo | undefined;
  sender: Sender<Echo> | undefined;
}

class RecordingConsumer extends ServiceStream implements TypedStreamConsumer<Echo> {
  readonly values: bigint[] = [];

  public consume(_context: MessageContext, value: Echo): void {
    this.values.push(value.value);
  }
}

class InteropHandler implements EndpointHandler<HandlerState, Echo, Echo, Echo, Echo, Error> {
  readonly #mode: DescMethod["methodKind"];

  public constructor(mode: DescMethod["methodKind"]) {
    this.#mode = mode;
  }

  public beginRequest(
    context: MessageContext
  ): Promise<{ readonly context: MessageContext; readonly state: HandlerState }> {
    assert.equal(context.streamId(), "go-official-client-stream");
    assert.equal(context.metadata().get("baggage"), "interop=go-official-client");
    assert.equal(context.metadata().has("x-interop-header"), false);
    return Promise.resolve({ context, state: { last: undefined, sender: undefined } });
  }

  public async consumeMessage(
    context: MessageContext,
    stream: StreamContext<Echo, Echo, Error>,
    state: HandlerState,
    request: Echo,
    _result: ResultContext<HandlerState, Echo, Echo, Echo, Error>,
    sender: Sender<Echo>
  ): Promise<void> {
    if (request.text === "fail") throw new Error("requested interoperability failure");
    state.last = request;
    state.sender = sender;
    await stream.collect(context, request);
    if (this.#mode === "unary" || this.#mode === "bidi_streaming") {
      await sender.send(context, request);
    } else if (this.#mode === "server_streaming") {
      await sender.send(context, request);
      await sender.send(context, { ...request, value: request.value + 1n });
      await sender.send(context, { ...request, value: request.value + 2n });
    }
  }

  public getMessageId(): string {
    return "unused";
  }

  public async eof(
    context: MessageContext,
    _stream: StreamContext<Echo, Echo, Error>,
    state: HandlerState
  ): Promise<void> {
    if (
      this.#mode === "client_streaming" &&
      state.last !== undefined &&
      state.sender !== undefined
    ) {
      await state.sender.send(context, state.last);
    }
  }

  public endRequest(): void {}
}

const connector: GrpcDataConnectorConfig = {
  id: 701,
  name: "cross-language",
  properties: {},
  type: 2,
  implementation: "grpc/grpc-js",
  connectionsCount: 1
};

await test(
  "official generated Go client interoperates with all TypeScript gRPC method modes",
  { skip: process.env["SERVICEGEN_RUN_CROSS_LANGUAGE_GRPC"] !== "1" },
  async () => {
    const methods = [
      InteropService.method.unary,
      InteropService.method.clientStreaming,
      InteropService.method.serverStreaming,
      InteropService.method.bidirectionalStreaming
    ];
    const endpointConfigs: GrpcEndpointConfig[] = methods.map((method, index) => ({
      id: 710 + index,
      name: method.localName,
      properties: {},
      idDataConnector: connector.id,
      grpcMethodType:
        method.methodKind === "unary"
          ? "NoStreaming"
          : method.methodKind === "client_streaming"
            ? "ClientStreaming"
            : method.methodKind === "server_streaming"
              ? "ServerStreaming"
              : "BidirectionalStreaming",
      methodName: method.name
    }));
    const streamConfigs: InputStreamConfig[] = endpointConfigs.map((endpoint, index) => ({
      id: 720 + index,
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
      valueType: "interop-echo"
    }));
    const environment = makeTestEnvironment(streamConfigs, {
      dataConnectors: [connector],
      endpoints: endpointConfigs,
      service: { grpcPort: 19212 }
    });
    registerTestSerdeType<Echo>(
      environment,
      "interop-echo",
      (value): value is Echo => typeof value === "object" && value !== null
    );
    const consumers: RecordingConsumer[] = [];
    for (const [index, streamConfig] of streamConfigs.entries()) {
      const method = methods[index];
      assert.ok(method);
      environment.serdeRegistry().registerStreamErrorType(streamConfig.id, errorSerdeType);
      const stream = makeInputStream<Echo, Echo, Error>(streamConfig, environment);
      const consumerConfig: StreamConfig = {
        ...streamConfig,
        id: streamConfig.id + 10,
        name: `${streamConfig.name}Consumer`,
        type: "Sink",
        idSource: streamConfig.id
      };
      const consumer = new RecordingConsumer(consumerConfig, environment);
      consumers.push(consumer);
      stream.setConsumer(consumer);
      const handler = new InteropHandler(method.methodKind);
      if (method.methodKind === "unary") {
        makeGrpcNoStreamingEndpointConsumer(stream, InteropService, method, handler);
      } else if (method.methodKind === "client_streaming") {
        makeGrpcClientStreamingEndpointConsumer(stream, InteropService, method, handler);
      } else if (method.methodKind === "server_streaming") {
        makeGrpcServerStreamingEndpointConsumer(stream, InteropService, method, handler);
      } else {
        makeGrpcBidiStreamingEndpointConsumer(stream, InteropService, method, handler);
      }
    }

    const source = environment.dataSourceById(connector.id);
    assert.ok(source);
    await source.start(Context.background());
    try {
      const { stdout, stderr } = await execFileAsync(
        "go",
        ["run", "./client", "-address", "127.0.0.1:19212"],
        {
          cwd: join(process.cwd(), "test", "interop"),
          env: {
            ...process.env,
            GOCACHE: "/tmp/servicegen-go-build",
            GOWORK: "off"
          },
          timeout: 60_000
        }
      );
      assert.equal(stderr, "");
      const report = JSON.parse(stdout) as {
        readonly status: string;
        readonly methods: readonly string[];
        readonly recoveryStatus: string;
      };
      assert.equal(report.status, "pass");
      assert.deepEqual(report.methods, [
        "unary",
        "client-streaming",
        "server-streaming",
        "bidirectional-streaming"
      ]);
      assert.equal(report.recoveryStatus, "OK");
      assert.deepEqual(
        consumers.map((consumer) => consumer.values),
        [[1n, 9n], [1n, 2n, 3n], [10n], [4n, 5n]]
      );
    } finally {
      await source.stop(Context.background().bounded(1_000));
    }
  }
);
