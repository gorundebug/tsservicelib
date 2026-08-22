import assert from "node:assert/strict";
import type { IncomingMessage, ServerResponse } from "node:http";
import { test } from "node:test";

import {
  registerRuntimeHTTPHandlers,
  NoopMetricsEngine,
  type HTTPHandler,
  type RuntimeEnvironment
} from "@gorundebug/tsservicelib/runtime";
import { makeTestEnvironment } from "./support/environment.js";

interface CapturedResponse {
  readonly statusCode: number;
  readonly headers: ReadonlyMap<string, string>;
  readonly body: string;
}

function captureEnvironment(): {
  readonly environment: RuntimeEnvironment;
  readonly handlers: ReadonlyMap<string, HTTPHandler>;
} {
  const base = makeTestEnvironment([]);
  const handlers = new Map<string, HTTPHandler>();
  base.registerHttpHandler = (path: string, handler: HTTPHandler): void => {
    handlers.set(path, handler);
  };
  return { environment: base, handlers };
}

function handler(handlers: ReadonlyMap<string, HTTPHandler>, path: string): HTTPHandler {
  const registered = handlers.get(path);
  assert.ok(registered, `HTTP handler ${path} is not registered`);
  return registered;
}

function invoke(handler: HTTPHandler, method = "GET"): CapturedResponse {
  const headers = new Map<string, string>();
  let body = "";
  const request = { method } as IncomingMessage;
  const responseState = {
    statusCode: 0,
    headersSent: false,
    writableEnded: false,
    setHeader(name: string, value: string): void {
      headers.set(name.toLowerCase(), value);
    },
    end(value = ""): void {
      body += value;
      this.headersSent = true;
      this.writableEnded = true;
    }
  };
  const response = responseState as unknown as ServerResponse;
  handler(request, response);
  return { statusCode: response.statusCode, headers, body };
}

await test("status routes serve the canonical embedded topology UI", () => {
  const { environment, handlers } = captureEnvironment();
  registerRuntimeHTTPHandlers(environment, new NoopMetricsEngine());

  assert.deepEqual(
    [...handlers.keys()],
    [
      "/status",
      "/status/data",
      "/status/graph",
      "/status/vis.min.js",
      "/status/vis.min.css",
      "/metrics",
      "/health/startup",
      "/health/ready",
      "/health/live"
    ]
  );

  const status = invoke(handler(handlers, "/status"));
  assert.equal(status.statusCode, 200);
  assert.equal(status.headers.get("content-type"), "text/html");
  assert.match(status.body, /new vis\.Network/);
  assert.match(status.body, /statusBase \+ '\/data'/);

  const javascript = invoke(handler(handlers, "/status/vis.min.js"));
  assert.equal(javascript.statusCode, 200);
  assert.equal(javascript.headers.get("content-type"), "application/javascript");
  assert.equal(javascript.headers.get("cache-control"), "public, max-age=31536000, immutable");
  assert.ok(javascript.body.length > 600_000);
  assert.match(javascript.body, /A dynamic, browser-based visualization library/);

  const stylesheet = invoke(handler(handlers, "/status/vis.min.css"));
  assert.equal(stylesheet.statusCode, 200);
  assert.equal(stylesheet.headers.get("content-type"), "text/css");
  assert.ok(stylesheet.body.length > 20_000);
  assert.match(stylesheet.body, /\.vis-network/);

  const data = invoke(handler(handlers, "/status/data"));
  assert.equal(data.headers.get("content-type"), "application/json");
  assert.deepEqual(JSON.parse(data.body), { nodes: [], edges: [] });

  const graph = invoke(handler(handlers, "/status/graph"));
  assert.equal(graph.headers.get("content-type"), "text/yaml; charset=utf-8");
  assert.match(graph.body, /name: test-service/);

  for (const path of ["/health/startup", "/health/ready", "/health/live"]) {
    const health = invoke(handler(handlers, path));
    assert.equal(health.statusCode, 200);
    assert.equal(health.body, "ok\n");
  }

  const rejected = invoke(handler(handlers, "/status"), "POST");
  assert.equal(rejected.statusCode, 405);
  assert.equal(rejected.body, "");
});
