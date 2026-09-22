import assert from "node:assert/strict";
import { createServer } from "node:net";
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
  Client,
  credentials,
  type handleUnaryCall,
  type handleClientStreamingCall,
  type handleServerStreamingCall,
  type handleBidiStreamingCall,
  type UntypedHandleCall
} from "@grpc/grpc-js";
import { GrpcJsDataSource } from "@gorundebug/tsservicelib/datasource/grpc";
import { Context, type GrpcDataConnectorConfig } from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

function descriptor(name: string, kind: DescMethod["methodKind"]): DescService {
  const service = {
    kind: "service",
    typeName: `test.${name}`,
    name,
    file: TimestampSchema.file,
    methods: [],
    method: {},
    deprecated: false,
    proto: {},
    toString: () => name
  } as unknown as DescService;
  for (const name of ["First", "Second"]) {
    const method = {
      kind: "rpc",
      name,
      localName: name.toLowerCase(),
      parent: service,
      methodKind: kind,
      input: TimestampSchema,
      output: TimestampSchema,
      deprecated: false,
      proto: {},
      toString: () => name
    } as unknown as DescMethod;
    service.methods.push(method);
    service.method[method.localName] = method;
  }
  return service;
}

async function fixture(): Promise<{
  first: GrpcJsDataSource;
  second: GrpcJsDataSource;
  client: Client;
}> {
  const reservation = createServer();
  await new Promise<void>((resolve, reject) => {
    reservation.once("error", reject);
    reservation.listen(0, "127.0.0.1", resolve);
  });
  const address = reservation.address();
  assert.ok(address !== null && typeof address !== "string");
  await new Promise<void>((resolve, reject) => {
    reservation.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
  const connectors: GrpcDataConnectorConfig[] = [10, 20].map((id) => ({
    id,
    name: `connector${String(id)}`,
    properties: {},
    type: 2,
    implementation: "grpc/grpc-js",
    connectionsCount: 1
  }));
  const environment = makeTestEnvironment([], {
    dataConnectors: connectors,
    service: { grpcPort: address.port }
  });
  return {
    first: new GrpcJsDataSource(10, environment),
    second: new GrpcJsDataSource(20, environment),
    client: new Client(`127.0.0.1:${String(address.port)}`, credentials.createInsecure())
  };
}

const encode = (value: Timestamp): Buffer => Buffer.from(toBinary(TimestampSchema, value));
const decode = (value: Buffer): Timestamp => fromBinary(TimestampSchema, value);
const request = create(TimestampSchema, { seconds: 42n });

function invoke(client: Client, method: DescMethod): Promise<Timestamp> {
  const path = `/${method.parent.typeName}/${method.name}`;
  const options = { deadline: Date.now() + 2_000 };
  return new Promise((resolve, reject) => {
    const callback = (error: Error | null, value?: Timestamp): void => {
      if (error !== null) reject(error);
      else if (value === undefined) reject(new Error("missing response"));
      else resolve(value);
    };
    if (method.methodKind === "unary") {
      client.makeUnaryRequest(path, encode, decode, request, options, callback);
    } else if (method.methodKind === "client_streaming") {
      const call = client.makeClientStreamRequest(path, encode, decode, options, callback);
      call.end(request);
    } else {
      const bidi =
        method.methodKind === "bidi_streaming"
          ? client.makeBidiStreamRequest(path, encode, decode, options)
          : undefined;
      const call =
        method.methodKind === "server_streaming"
          ? client.makeServerStreamRequest(path, encode, decode, request, options)
          : bidi;
      assert.ok(call);
      let result: Timestamp | undefined;
      call.on("data", (value: Timestamp) => {
        result = value;
      });
      call.on("error", reject);
      call.on("end", () => {
        callback(null, result);
      });
      bidi?.end(request);
    }
  });
}

function echo(kind: DescMethod["methodKind"]): UntypedHandleCall {
  if (kind === "unary") {
    const handler: handleUnaryCall<Timestamp, Timestamp> = (call, callback) => {
      callback(null, call.request);
    };
    return handler;
  }
  if (kind === "client_streaming") {
    const handler: handleClientStreamingCall<Timestamp, Timestamp> = (call, callback) => {
      let value: Timestamp | undefined;
      call.on("data", (request: Timestamp) => {
        value = request;
      });
      call.on("end", () => {
        callback(null, value);
      });
    };
    return handler;
  }
  if (kind === "server_streaming") {
    const handler: handleServerStreamingCall<Timestamp, Timestamp> = (call) => {
      call.write(call.request);
      call.end();
    };
    return handler;
  }
  const handler: handleBidiStreamingCall<Timestamp, Timestamp> = (call) => {
    call.on("data", (request: Timestamp) => {
      call.write(request);
    });
    call.on("end", () => {
      call.end();
    });
  };
  return handler;
}

for (const kind of ["unary", "client_streaming", "server_streaming", "bidi_streaming"] as const) {
  for (const sharedDescriptor of [false, true]) {
    await test(`gRPC connectors share listener: ${kind}, shared descriptor ${String(sharedDescriptor)}`, async () => {
      const { first, second, client } = await fixture();
      const firstService = descriptor("FirstService", kind);
      const secondService = sharedDescriptor ? firstService : descriptor("SecondService", kind);
      const firstMethod = firstService.methods[0];
      const secondMethod = secondService.methods[1];
      assert.ok(firstMethod && secondMethod);
      first.add(firstService, firstMethod, echo(kind));
      second.add(secondService, secondMethod, echo(kind));
      try {
        await Promise.all([first.start(Context.background()), second.start(Context.background())]);
        assert.equal((await invoke(client, firstMethod)).seconds, 42n);
        assert.equal((await invoke(client, secondMethod)).seconds, 42n);
        await first.stop(Context.background().bounded(500));
        await assert.rejects(invoke(client, firstMethod));
        assert.equal((await invoke(client, secondMethod)).seconds, 42n);
        await first.start(Context.background());
        assert.equal((await invoke(client, firstMethod)).seconds, 42n);
      } finally {
        client.close();
        await Promise.all([
          first.stop(Context.background().bounded(500)),
          second.stop(Context.background().bounded(500))
        ]);
      }
    });
  }
}

for (const forced of [false, true]) {
  await test(`gRPC connector drains only its own calls, forced ${String(forced)}`, async () => {
    const { first, second, client } = await fixture();
    const service = descriptor("DrainService", "unary");
    const firstMethod = service.methods[0];
    const secondMethod = service.methods[1];
    assert.ok(firstMethod && secondMethod);
    let enter = (): void => undefined;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    let release = (): void => undefined;
    let cancelled = false;
    const handler: handleUnaryCall<Timestamp, Timestamp> = (call, callback) => {
      call.once("cancelled", () => {
        cancelled = true;
      });
      release = () => {
        callback(null, call.request);
      };
      enter();
    };
    first.add(service, firstMethod, handler);
    second.add(service, secondMethod, echo("unary"));
    try {
      await first.start(Context.background());
      await second.start(Context.background());
      const response = invoke(client, firstMethod).then(
        (value) => value,
        (error: unknown) => error
      );
      await entered;
      let stopped = false;
      const stopping = first
        .stop(forced ? Context.background().bounded(20) : Context.background())
        .then(() => {
          stopped = true;
        });
      assert.equal((await invoke(client, secondMethod)).seconds, 42n);
      await assert.rejects(invoke(client, firstMethod));
      if (!forced) {
        assert.equal(stopped, false);
        release();
      }
      await stopping;
      const outcome = await response;
      if (forced) {
        assert.ok(outcome instanceof Error);
        assert.equal(cancelled, true);
      } else {
        assert.deepEqual(outcome, request);
      }
      assert.equal((await invoke(client, secondMethod)).seconds, 42n);
    } finally {
      client.close();
      await first.stop(Context.background().bounded(100));
      await second.stop(Context.background().bounded(100));
    }
  });
}

await test("duplicate RPC registration does not remove another connector's method", async () => {
  const { first, second, client } = await fixture();
  const service = descriptor("DuplicateService", "unary");
  const method = service.methods[0];
  assert.ok(method);
  first.add(service, method, echo("unary"));
  second.add(service, method, echo("unary"));
  try {
    await first.start(Context.background());
    await assert.rejects(second.start(Context.background()));
    assert.equal((await invoke(client, method)).seconds, 42n);
  } finally {
    client.close();
    await first.stop(Context.background().bounded(100));
    await second.stop(Context.background().bounded(100));
  }
});
