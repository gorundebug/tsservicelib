import type { IncomingMessage, ServerResponse } from "node:http";
import { readFileSync } from "node:fs";

import { stringify } from "yaml";

import type { RuntimeEnvironment } from "../environment/index.js";
import type { MetricsEngine } from "../environment/metrics/index.js";
import { makeStatusNetworkData, runtimeToCanonicalConfig } from "./graph.js";

const STATUS_HTML = readStatusAsset("status.html");
const VIS_JS = readStatusAsset("vis.min.js");
const VIS_CSS = readStatusAsset("vis.min.css");

function readStatusAsset(name: string): string {
  return readFileSync(new URL(`./web/${name}`, import.meta.url), "utf8");
}

function path(value: string): string {
  return `/${value.replace(/^\/+|\/+$/gu, "")}`;
}

function requireGet(request: IncomingMessage, response: ServerResponse): boolean {
  if (request.method === "GET") return true;
  response.statusCode = 405;
  response.end();
  return false;
}

function send(
  request: IncomingMessage,
  response: ServerResponse,
  contentType: string,
  body: string,
  immutable = false
): void {
  if (!requireGet(request, response)) return;
  response.statusCode = 200;
  response.setHeader("content-type", contentType);
  if (immutable) response.setHeader("cache-control", "public, max-age=31536000, immutable");
  response.end(body);
}

export function registerRuntimeHTTPHandlers(
  environment: RuntimeEnvironment,
  metricsEngine: MetricsEngine & {
    contentType(): string;
    render(): Promise<string>;
  }
): void {
  const service = environment.serviceConfig();
  if (service.statusHandler.length > 0) {
    const base = path(service.statusHandler);
    environment.registerHttpHandler(base, (request, response) => {
      send(request, response, "text/html", STATUS_HTML);
    });
    environment.registerHttpHandler(`${base}/data`, (request, response) => {
      send(
        request,
        response,
        "application/json",
        JSON.stringify(makeStatusNetworkData(environment))
      );
    });
    environment.registerHttpHandler(`${base}/graph`, (request, response) => {
      send(
        request,
        response,
        "text/yaml; charset=utf-8",
        stringify(runtimeToCanonicalConfig(environment))
      );
    });
    environment.registerHttpHandler(`${base}/vis.min.js`, (request, response) => {
      send(request, response, "application/javascript", VIS_JS, true);
    });
    environment.registerHttpHandler(`${base}/vis.min.css`, (request, response) => {
      send(request, response, "text/css", VIS_CSS, true);
    });
  }
  if (service.metricsHandler.length > 0) {
    environment.registerHttpHandler(path(service.metricsHandler), (request, response) => {
      if (!requireGet(request, response)) return;
      void metricsEngine
        .render()
        .then((body) => {
          response.statusCode = 200;
          response.setHeader("content-type", metricsEngine.contentType());
          response.end(body);
        })
        .catch((error: unknown) => {
          if (!response.headersSent) response.statusCode = 500;
          if (!response.writableEnded) response.end("metrics rendering failed");
          void error;
        });
    });
  }
  const healthPaths = new Set(
    [service.startupHandler, service.readinessHandler, service.livenessHandler]
      .filter((configured) => configured.length > 0)
      .map(path)
  );
  for (const healthPath of healthPaths) {
    environment.registerHttpHandler(healthPath, (request, response) => {
      send(request, response, "text/plain; charset=utf-8", "ok\n");
    });
  }
}
