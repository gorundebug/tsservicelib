import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { test } from "node:test";
import { S3Client } from "@aws-sdk/client-s3";
import * as query from "../dist/dataquery/s3aws/index.js";

function fixture() {
  const abort = new AbortController();
  const values = [],
    errors = [];
  return {
    abort,
    values,
    errors,
    context: { signal: () => abort.signal },
    stream: {
      consumeResult: async (_ctx, value) => {
        values.push(value);
      },
      errorStream: () => ({
        emit: async (_ctx, error) => {
          errors.push(error);
        }
      }),
      runtimeEnvironment: () => ({ metrics: () => ({ enabled: () => false }) }), setQueryConsumer: () => {}
    }
  };
}

test("official S3 client: all Query operations, pagination and cleanup", async () => {
  const server = createServer(async (request, response) => {
    const url = new URL(request.url, "http://local");
    if (url.searchParams.has("list-type")) {
      response.setHeader("Content-Type", "application/xml");
      response.end(
        url.searchParams.has("continuation-token")
          ? "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>second</Key></Contents></ListBucketResult>"
          : "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>first</Key></Contents></ListBucketResult>"
      );
    } else if (request.method === "GET") response.end("object contents");
    else if (request.method === "PUT") {
      for await (const _ of request) {
      }
      response.end();
    } else if (request.method === "DELETE") {
      response.statusCode = 204;
      response.end();
    } else response.end();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const client = new S3Client({
    endpoint: `http://127.0.0.1:${server.address().port}`,
    region: "us-east-1",
    forcePathStyle: true,
    maxAttempts: 1,
    credentials: { accessKeyId: "fixture", secretAccessKey: "fixture" }
  });
  try {
    for (const operation of [
      "GetObject",
      "HeadObject",
      "ListObjects",
      "PutObject",
      "DeleteObject"
    ]) {
      const f = fixture();
      let ends = 0;
      let body;
      const handler = {
        beginRequest: (context) => ({ context, state: 42 }),
        buildRequest: () => ({
          Bucket: "bucket",
          Key: "key",
          ...(operation === "PutObject" ? { Body: "upload" } : {})
        }),
        handleResult: async (ctx, state, result, out) => {
          assert.equal(state, 42);
          if (result.Body) {
            body = result.Body;
            assert.equal(await body.transformToString(), "object contents");
          }
          await out.out(ctx, operation === "ListObjects" ? result.Contents[0].Key : operation);
        },
        endRequest: (_ctx, _stream, state, error) => {
          ends++;
          assert.equal(state, 42);
          assert.equal(error, undefined);
          if (body) assert.equal(body.destroyed, true);
        }
      };
      await query[`make${operation}EndpointConsumer`](f.stream, client, handler).consume(
        f.context,
        "request"
      );
      assert.equal(ends, 1);
      assert.deepEqual(f.values, operation === "ListObjects" ? ["first", "second"] : [operation]);
    }
    const f = fixture();
    let body;
    let ends = 0;
    const failure = new Error("mapper failed");
    await query
      .makeGetObjectEndpointConsumer(f.stream, client, {
        beginRequest: (context) => ({ context, state: 1 }),
        buildRequest: () => ({ Bucket: "bucket", Key: "key" }),
        handleResult: (_ctx, _state, result) => {
          body = result.Body;
          throw failure;
        },
        endRequest: (_ctx, _stream, _state, error) => {
          ends++;
          assert.equal(body.destroyed, true);
          assert.equal(error, failure);
        }
      })
      .consume(f.context, "request");
    assert.equal(ends, 1);
    assert.deepEqual(f.values, []);
  } finally {
    client.destroy();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Query rejects native retries and finalizes a cancelled request once", async () => {
  const client = new S3Client({ region: "us-east-1", maxAttempts: 3 });
  try {
    for (const cancelled of [false, true]) {
      const f = fixture();
      let builds = 0;
      let ends = 0;
      if (cancelled) f.abort.abort(new Error("cancelled"));
      await query
        .makeDeleteObjectEndpointConsumer(f.stream, client, {
          beginRequest: (context) => ({ context, state: 1 }),
          buildRequest: () => {
            builds++;
            return { Bucket: "bucket", Key: "key" };
          },
          handleResult: () => assert.fail("unexpected native result"),
          endRequest: (_ctx, _stream, _state, error) => {
            ends++;
            assert.match(error.message, cancelled ? /cancelled/ : /maxAttempts/);
          }
        })
        .consume(f.context, "request");
      assert.equal(builds, 0);
      assert.equal(ends, 1);
    }
  } finally {
    client.destroy();
  }
});

test("S3 empty objects/listings remain distinct from missing and forbidden objects", async () => {
  const requests = [];
  const server = createServer((request, response) => {
    const url = new URL(request.url, "http://local");
    requests.push(`${request.method} ${url.pathname}`);
    if (url.searchParams.has("list-type")) {
      response.setHeader("Content-Type", "application/xml");
      response.end("<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>");
    } else if (url.pathname.endsWith("/missing") || url.pathname.endsWith("/forbidden")) {
      const missing = url.pathname.endsWith("/missing");
      response.statusCode = missing ? 404 : 403;
      response.setHeader("Content-Type", "application/xml");
      response.end(`<Error><Code>${missing ? "NoSuchKey" : "AccessDenied"}</Code></Error>`);
    } else {
      response.setHeader("Content-Length", "0");
      response.end();
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const client = new S3Client({
    endpoint: `http://127.0.0.1:${server.address().port}`, region: "us-east-1",
    forcePathStyle: true, maxAttempts: 1,
    credentials: { accessKeyId: "fixture", secretAccessKey: "fixture" }
  });
  try {
    for (const operation of ["GetObject", "HeadObject", "ListObjects"]) {
      for (const key of (operation === "ListObjects" ? ["empty"] : ["empty", "missing", "forbidden"])) {
        const f = fixture();
        let mapped = 0, ends = 0, body;
        const before = requests.length;
        await query[`make${operation}EndpointConsumer`](f.stream, client, {
          beginRequest: context => ({ context, state: key }),
          buildRequest: () => ({ Bucket: "bucket", ...(operation === "ListObjects" ? { Prefix: "absent/" } : { Key: key }) }),
          handleResult: async (ctx, state, result, out) => {
            mapped++;
            assert.equal(state, "empty");
            if (operation === "GetObject") {
              body = result.Body;
              assert.equal(await body.transformToString(), "");
            } else if (operation === "HeadObject") assert.equal(result.ContentLength, 0);
            else assert.deepEqual(result.Contents ?? [], []);
            await out.out(ctx, "empty-success");
          },
          endRequest: async (ctx, _stream, state, error, out, errors) => {
            ends++;
            assert.equal(state, key);
            if (body) assert.equal(body.destroyed, true);
            if (key === "empty") {
              assert.equal(error, undefined);
              await out.out(ctx, "completed");
            } else {
              assert.equal(error.$metadata.httpStatusCode, key === "missing" ? 404 : 403);
              await errors.out(ctx, error);
            }
          }
        }).consume(f.context, key);
        assert.equal(ends, 1);
        assert.equal(mapped, key === "empty" ? 1 : 0);
        assert.deepEqual(f.values, key === "empty" ? ["empty-success", "completed"] : []);
        assert.equal(f.errors.length, key === "empty" ? 0 : 1);
        assert.equal(requests.length - before, 1, "no hidden retries");
      }
    }
  } finally {
    client.destroy();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});

test("large native S3 body streams before completion and cleans up on every outcome", { timeout: 15000 }, async () => {
  const size = 16 * 1024 * 1024;
  for (const mode of ["success", "mapper-error", "cancel"]) {
    const f = fixture();
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    let calls = 0;
    const server = createServer(async (_request, response) => {
      calls++;
      response.setHeader("Content-Length", size);
      response.write(Buffer.alloc(4096, 120));
      await gate;
      try {
        const chunk = Buffer.alloc(64 * 1024, 120);
        for (let left = size - 4096; left > 0 && !response.destroyed;) {
          const count = Math.min(left, chunk.length);
          if (!response.write(chunk.subarray(0, count))) {
            await new Promise(resolve => {
              const done = () => {
                response.off("drain", done);
                response.off("close", done);
                resolve();
              };
              response.once("drain", done);
              response.once("close", done);
            });
          }
          left -= count;
        }
        if (!response.destroyed) response.end();
      } catch { /* Expected connection close after mapper failure/cancellation. */ }
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const client = new S3Client({
      endpoint: `http://127.0.0.1:${server.address().port}`,
      region: "us-east-1", forcePathStyle: true, maxAttempts: 1,
      credentials: { accessKeyId: "fixture", secretAccessKey: "fixture" }
    });
    let body, received = 0, ends = 0, outcome;
    const failure = new Error("mapper stopped");
    const timeout = setTimeout(() => f.abort.abort(), 5000);
    try {
      const handler = {
        beginRequest: context => ({ context, state: 0 }),
        buildRequest: () => ({ Bucket: "bucket", Key: "large" }),
        handleResult: async (ctx, _state, result, out) => {
          body = result.Body;
          for await (const chunk of body) {
            assert.ok(chunk.every(value => value === 120));
            received += chunk.length;
            if (received === chunk.length) {
              // Server cannot complete until native output reaches this mapper.
              release();
              if (mode === "mapper-error") throw failure;
              if (mode === "cancel") { f.abort.abort(); break; }
            }
          }
          if (mode === "success") await out.out(ctx, received);
        },
        endRequest: (_ctx, _stream, _state, error) => {
          ends++; outcome = error;
          assert.equal(body?.destroyed, true);
        }
      };
      await query.makeGetObjectEndpointConsumer(f.stream, client, handler).consume(f.context, "large");
      assert.equal(calls, 1);
      assert.equal(ends, 1);
      if (mode === "success") {
        assert.equal(outcome, undefined);
        assert.equal(received, size);
        assert.deepEqual(f.values, [size]);
      } else {
        assert.ok(outcome instanceof Error);
        if (mode === "mapper-error") assert.equal(outcome, failure);
        assert.deepEqual(f.values, []);
      }
    } finally {
      clearTimeout(timeout);
      release();
      client.destroy();
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
    }
  }
});
