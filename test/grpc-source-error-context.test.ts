import assert from "node:assert/strict";
import { Duplex, Readable } from "node:stream";
import { test } from "node:test";

import { create, type DescMethod, type DescService } from "@bufbuild/protobuf";
import { TimestampSchema, type Timestamp } from "@bufbuild/protobuf/wkt";
import { Metadata } from "@grpc/grpc-js";
import {
  makeGrpcNoStreamingEndpointConsumer,
  makeGrpcClientStreamingEndpointConsumer,
  makeGrpcServerStreamingEndpointConsumer,
  makeGrpcBidiStreamingEndpointConsumer,
  type EndpointHandler
} from "@gorundebug/tsservicelib/datasource/grpc";
import { makeInputStream } from "@gorundebug/tsservicelib/operators";
import {
  STREAM_ID_HEADER,
  errorSerdeType,
  type Context,
  type MessageContext,
  type InputStreamConfig,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

const modes = [
  ["unary", "NoStreaming", makeGrpcNoStreamingEndpointConsumer],
  ["client_streaming", "ClientStreaming", makeGrpcClientStreamingEndpointConsumer],
  ["server_streaming", "ServerStreaming", makeGrpcServerStreamingEndpointConsumer],
  ["bidi_streaming", "BidirectionalStreaming", makeGrpcBidiStreamingEndpointConsumer]
] as const;

for (const [mode, grpcMethodType, make] of modes) {
  for (const transportThrows of [false, true]) {
    await test(`${mode} failure logging retains RPC context, transportThrows=${String(transportThrows)}`,
      { timeout: 2000 }, async (t) => {
        const config: InputStreamConfig = {
          id: 1, name: "input", type: "Input", properties: {}, pipeline: "main",
          idService: 1, idSource: 0, idSources: [], xPos: 0, yPos: 0,
          idEndpoint: 100, valueType: "timestamp"
        };
        const endpoint: GrpcEndpointConfig = {
          id: 100, name: "rpc", properties: {}, idDataConnector: 10,
          grpcMethodType, methodName: "Call"
        };
        const connector: GrpcDataConnectorConfig = {
          id: 10, name: "grpc", type: 2, properties: {},
          implementation: "grpc/grpc-js", connectionsCount: 1
        };
        const environment = makeTestEnvironment([config], {
          endpoints: [endpoint], dataConnectors: [connector]
        });
        registerTestSerdeType<Timestamp>(environment, "timestamp",
          (value): value is Timestamp => typeof value === "object" && value !== null);
        environment.serdeRegistry().registerStreamErrorType(1, errorSerdeType);
        const input = makeInputStream<Timestamp, Timestamp, Error>(config, environment);
        const handler: EndpointHandler<undefined, Timestamp, Timestamp, Timestamp, Timestamp, Error> = {
          beginRequest: (context) => ({ context, state: undefined }),
          consumeMessage: (context, _stream, _state, request, _result, sender) => sender.send(context, request),
          getMessageId: () => "message",
          eof: () => undefined,
          endRequest: () => undefined
        };
        const service = {
          kind: "service", name: "Test", typeName: "test.Test", methods: [], method: {},
          file: TimestampSchema.file
        } as unknown as DescService;
        const method = {
          kind: "rpc", name: "Call", localName: "call", parent: service,
          methodKind: mode, input: TimestampSchema, output: TimestampSchema
        } as unknown as DescMethod;
        service.methods.push(method);
        service.method["call"] = method;
        const consumer = make(input, service, method, handler);
        const source = environment.dataSourceById(connector.id);
        assert.ok(source);
        const runtimeEndpoint = source.endpoint(endpoint.id);
        assert.ok(runtimeEndpoint);
        const request = create(TimestampSchema, { seconds: 7n });
        const metadata = new Metadata();
        metadata.set(STREAM_ID_HEADER, "failed-rpc-owner");
        let read = false;
        const requestOptions = {
          objectMode: true,
          read(this: Readable) {
            if (read) return;
            read = true;
            this.push(request);
            this.push(null);
          }
        };
        const call = Object.assign(
          mode === "client_streaming"
            ? new Readable(requestOptions)
            : new Duplex({
                ...requestOptions,
                write(_value, _encoding, done) { done(); }
              }),
          { metadata, request, getDeadline: () => Date.now() + 10_000 }
        );
        call.on("error", () => undefined);
        const failure = new Error("request instrumentation failed");
        const transportFailure = new Error("transport failure reporting failed");
        let requestContext: MessageContext | undefined;
        t.mock.method(runtimeEndpoint, "onRequestEnd", (context: MessageContext) => {
          requestContext = context;
          call.emit("cancelled");
          throw failure;
        });
        const observed = Promise.withResolvers<undefined>();
        const logs: { context: Context; message: string }[] = [];
        t.mock.method(environment.log(), "error", (context: Context, message: string) => {
          logs.push({ context, message });
          if (logs.length === (transportThrows ? 2 : 1)) observed.resolve(undefined);
        });
        if (transportThrows && (mode === "server_streaming" || mode === "bidi_streaming")) {
          t.mock.method(call, "destroy", () => { throw transportFailure; });
        }
        const handle = (consumer as unknown as {
          handle(): (call: unknown, callback: (error: Error | null) => void) => void;
        }).handle();
        try {
          handle(call, () => { if (transportThrows) throw transportFailure; });
          await observed.promise;
          assert.ok(requestContext);
          assert.deepEqual(logs.map((entry) => entry.message), transportThrows
            ? ["gRPC request handler failed", "gRPC request failure reporting failed"]
            : ["gRPC request handler failed"]);
          for (const entry of logs) {
            assert.equal((entry.context as MessageContext).streamId(), "failed-rpc-owner");
            assert.equal(entry.context.signal(), requestContext.signal());
            assert.equal(entry.context.deadline(), requestContext.deadline());
            assert.equal(entry.context.cancelled(), true);
          }
        } finally {
          t.mock.restoreAll();
          call.destroy();
        }
      });
  }
}
