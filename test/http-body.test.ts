import assert from "node:assert/strict";
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type Server,
  type ServerResponse
} from "node:http";
import { connect } from "node:net";
import { test } from "node:test";

import {
  InvalidJsonBodyError,
  RequestBodyTooLargeError,
  readRequestBody,
  readJsonBody,
  writeJsonResponse,
  writeRequestError
} from "@gorundebug/tsservicelib/datasource/http";

interface TestBody {
  readonly value: string;
}

await test("HTTP body helpers validate JSON and write a typed response", async () => {
  await withServer(64, async (baseUrl) => {
    const response = await fetch(baseUrl, {
      method: "POST",
      body: JSON.stringify({ value: "ok" })
    });
    assert.equal(response.status, 201);
    assert.equal(response.headers.get("content-type"), "application/json");
    assert.deepEqual(await response.json(), { accepted: "ok" });
  });
});

await test("HTTP body helpers map malformed JSON and validation failures to 400", async () => {
  await withServer(64, async (baseUrl) => {
    for (const body of ["{", JSON.stringify({ value: 1 })]) {
      const response = await fetch(baseUrl, { method: "POST", body });
      assert.equal(response.status, 400);
      assert.equal(await response.text(), "invalid JSON body\n");
    }
  });
});

await test("HTTP body helpers reject known and streaming oversize bodies with 413", async () => {
  await withServer(4, async (baseUrl) => {
    const known = await fetch(baseUrl, { method: "POST", body: "12345" });
    assert.equal(known.status, 413);
    assert.equal(await known.text(), "request body is too large\n");

    const streamed = await chunkedRequest(baseUrl, ["12", "345"]);
    assert.equal(streamed.statusCode, 413);
    assert.equal(streamed.body, "request body is too large\n");
  });
});

await test("HTTP body errors expose stable status and body-limit data", () => {
  const malformed = new InvalidJsonBodyError();
  assert.equal(malformed.statusCode, 400);
  const oversized = new RequestBodyTooLargeError(128);
  assert.equal(oversized.statusCode, 413);
  assert.equal(oversized.limit, 128);
});

await test(
  "HTTP body reader rejects an aborted upload without hanging",
  { timeout: 1000 },
  async () => {
    let resolveObserved: ((error: Error) => void) | undefined;
    const observed = new Promise<Error>((resolve) => {
      resolveObserved = resolve;
    });
    const server = createServer((request, response) => {
      void readRequestBody(request, 256).then(
        () => response.end(),
        (error: unknown) => {
          resolveObserved?.(error instanceof Error ? error : new Error(String(error)));
        }
      );
    });
    await listen(server);
    const address = server.address();
    if (address === null || typeof address === "string") {
      throw new Error("expected TCP server address");
    }
    const socket = connect(address.port, "127.0.0.1");
    await new Promise<void>((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    socket.write("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n12");
    socket.destroy();
    const error = await observed;
    assert.match(error.message, /aborted|reset/i);
    server.closeAllConnections();
    await close(server);
  }
);

async function withServer(
  bodyLimit: number,
  run: (baseUrl: string) => Promise<void>
): Promise<void> {
  const server = createServer((request, response) => {
    void serve(request, response, bodyLimit);
  });
  await listen(server);
  try {
    const address = server.address();
    assert.notEqual(address, null);
    assert.equal(typeof address, "object");
    if (address === null || typeof address === "string") {
      throw new Error("expected TCP server address");
    }
    await run(`http://127.0.0.1:${String(address.port)}/`);
  } finally {
    await close(server);
  }
}

async function serve(
  request: IncomingMessage,
  response: ServerResponse,
  bodyLimit: number
): Promise<void> {
  try {
    const body = await readJsonBody(request, decodeTestBody, bodyLimit);
    writeJsonResponse(response, 201, { accepted: body.value });
  } catch (error: unknown) {
    if (!writeRequestError(response, error)) {
      response.statusCode = 500;
      response.end("internal server error\n");
    }
  }
}

function decodeTestBody(value: unknown): TestBody {
  if (
    typeof value !== "object" ||
    value === null ||
    !("value" in value) ||
    typeof value.value !== "string"
  ) {
    throw new Error("value must be a string");
  }
  return { value: value.value };
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

function chunkedRequest(
  url: string,
  chunks: readonly string[]
): Promise<{ readonly statusCode: number; readonly body: string }> {
  return new Promise((resolve, reject) => {
    const request = httpRequest(url, { method: "POST" }, (response) => {
      const responseChunks: Uint8Array[] = [];
      response.on("data", (chunk: unknown) => {
        if (chunk instanceof Uint8Array) {
          responseChunks.push(chunk);
        }
      });
      response.once("end", () => {
        resolve({
          statusCode: response.statusCode ?? 0,
          body: Buffer.concat(responseChunks).toString("utf8")
        });
      });
    });
    request.once("error", reject);
    for (const chunk of chunks) {
      request.write(chunk);
    }
    request.end();
  });
}
