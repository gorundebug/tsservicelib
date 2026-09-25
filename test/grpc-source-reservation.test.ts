import assert from "node:assert/strict";
import { Duplex } from "node:stream";
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
  ConsumedStream,
  STREAM_ID_HEADER,
  errorSerdeType,
  type InputStreamConfig,
  type MessageContext,
  type GrpcDataConnectorConfig,
  type GrpcEndpointConfig
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment, registerTestSerdeType } from "./support/environment.js";

function gate(): { promise: Promise<void>; resolve(): void } {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error("RPC reservation test timed out"));
        }, 2000);
      })
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

const modes = [
  ["unary", "NoStreaming", makeGrpcNoStreamingEndpointConsumer],
  ["client_streaming", "ClientStreaming", makeGrpcClientStreamingEndpointConsumer],
  ["server_streaming", "ServerStreaming", makeGrpcServerStreamingEndpointConsumer],
  ["bidi_streaming", "BidirectionalStreaming", makeGrpcBidiStreamingEndpointConsumer]
] as const;

for (const [mode, grpcMethodType, make] of modes) {
  for (const hasResult of [false, true]) {
    for (const phase of ["consume", "end", "done", "callback"] as const) {
      if (phase === "done" && (mode !== "unary" || !hasResult)) continue;
      if (phase === "callback" && !hasResult) continue;
      await test(`${mode}: reserve ID through ${phase}, result=${String(hasResult)}`, async () => {
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
        const endpoint: GrpcEndpointConfig = {
          id: 100,
          name: "rpc",
          properties: {},
          idDataConnector: 10,
          grpcMethodType,
          methodName: "Call"
        };
        const connector: GrpcDataConnectorConfig = {
          id: 10,
          name: "grpc",
          type: 2,
          properties: {},
          implementation: "grpc/grpc-js",
          connectionsCount: 1
        };
        const environment = makeTestEnvironment([config], {
          endpoints: [endpoint],
          dataConnectors: [connector]
        });
        registerTestSerdeType<Timestamp>(
          environment,
          "timestamp",
          (value): value is Timestamp => typeof value === "object" && value !== null
        );
        environment.serdeRegistry().registerStreamErrorType(1, errorSerdeType);
        const input = makeInputStream<Timestamp, Timestamp, Error>(config, environment);
        if (hasResult)
          input.setSource(
            new ConsumedStream<Timestamp>(
              { ...config, id: 2, name: "result", type: "Map" },
              environment,
              input.serde()
            )
          );
        const entered = gate();
        const release = gate();
        let started = 0;
        let consumed = 0;
        let callbackCount = 0;
        let firstEnded = false;
        let resultWork: Promise<void> = Promise.resolve();
        let firstContext: MessageContext | undefined;
        let sendLateResponse = (): Promise<void> => Promise.resolve();
        const handler: EndpointHandler<number, Timestamp, Timestamp, Timestamp, Timestamp, Error> =
          {
            beginRequest(context) {
              return { context, state: ++started };
            },
            async consumeMessage(context, _stream, state, request, result, sender) {
              consumed++;
              if (state === 1) firstContext = context;
              if (state === 1 && phase === "callback") {
                result.setResultCallback("message", async () => {
                  callbackCount++;
                  result.done();
                  entered.resolve();
                  await release.promise;
                  await sender.send(context, request);
                  return true;
                });
                resultWork = Promise.resolve(input.consumeResult(context, request));
                return;
              }
              if (state === 1 && phase === "done") {
                sendLateResponse = () => {
                  sendLateResponse = () => Promise.resolve();
                  return Promise.resolve(sender.send(context, request));
                };
                result.done();
                entered.resolve();
                return;
              }
              if (state === 1 && phase === "consume") {
                entered.resolve();
                await release.promise;
              }
              await sender.send(context, request);
              result.done();
            },
            getMessageId() {
              return "message";
            },
            eof() {},
            async endRequest(_context, _stream, _failure, state) {
              if (state === 1) firstEnded = true;
              if (state === 1 && phase === "end") {
                entered.resolve();
                await release.promise;
              }
            }
          };
        const service = {
          kind: "service",
          name: "Test",
          typeName: "test.Test",
          methods: [],
          method: {},
          file: TimestampSchema.file
        } as unknown as DescService;
        const method = {
          kind: "rpc",
          name: "Call",
          localName: "call",
          parent: service,
          methodKind: mode,
          input: TimestampSchema,
          output: TimestampSchema
        } as unknown as DescMethod;
        service.methods.push(method);
        service.method["call"] = method;
        const consumer = make(input, service, method, handler);
        // Invoke the same public handler installed in grpc-js, without transport timing.
        const handle = (
          consumer as unknown as {
            handle(): (call: unknown, callback: (error: Error | null) => void) => void;
          }
        ).handle();
        const calls: Duplex[] = [];
        const invoke = (id: string): Promise<Error | undefined> => {
          const request = create(TimestampSchema, { seconds: 7n });
          const metadata = new Metadata();
          metadata.set(STREAM_ID_HEADER, id);
          let sent = false;
          const call = Object.assign(
            new Duplex({
              objectMode: true,
              read() {
                if (!sent) {
                  sent = true;
                  this.push(request);
                  this.push(null);
                }
              },
              write(_value, _encoding, done) {
                done();
              }
            }),
            { metadata, request, getDeadline: () => Infinity }
          );
          calls.push(call);
          return new Promise((resolve) => {
            call.on("error", (error: Error) => {
              resolve(error);
            });
            if (mode === "server_streaming" || mode === "bidi_streaming")
              call.on("finish", () => {
                resolve(undefined);
              });
            handle(call, (error) => {
              resolve(error ?? undefined);
            });
          });
        };
        const first = invoke("same-id");
        try {
          await within(entered.promise);
          if (phase === "callback") {
            await new Promise<void>((resolve) => setImmediate(resolve));
            assert.equal(firstEnded, false, "EndRequest overlapped an active result callback");
          }
          if (phase === "done") {
            let finished = false;
            void first.then(() => {
              finished = true;
            });
            await new Promise<void>((resolve) => setImmediate(resolve));
            assert.equal(finished, false, "unary Done completed the RPC without a response");
          }
          const duplicate = await within(invoke("same-id"));
          assert.match(duplicate?.message ?? "", /duplicate key/);
          assert.equal(consumed, 1, "duplicate reached business handler");
          assert.equal(await within(invoke("independent-id")), undefined);
          release.resolve();
          await sendLateResponse();
          await within(resultWork);
          assert.equal(await within(first), undefined);
          if (phase === "callback") {
            assert.ok(firstContext);
            await input.consumeResult(firstContext, create(TimestampSchema));
            assert.equal(callbackCount, 1, "late result entered a retired callback");
            assert.equal(firstEnded, true);
          }
          assert.equal(
            await within(invoke("same-id")),
            undefined,
            "ID not reusable after completion"
          );
          assert.equal(consumed, 3);
        } finally {
          release.resolve();
          await sendLateResponse();
          await within(resultWork);
          await within(first);
          for (const call of calls) call.destroy();
        }
      });
    }
  }
}
