import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import type { ServiceConfig } from "./config/index.js";
import type { Context } from "./context.js";
import type { Lifecycle } from "./lifecycle.js";

export type HTTPHandler = (request: IncomingMessage, response: ServerResponse) => void;

/** The service-wide HTTP listener shared by non-dedicated HTTP connectors. */
export class ServiceHTTPServer implements Lifecycle {
  readonly #config: () => ServiceConfig;
  readonly #routes = new Map<string, HTTPHandler>();
  #server: Server | undefined;

  public constructor(config: () => ServiceConfig) {
    this.#config = config;
  }

  public register(path: string, handler: HTTPHandler): void {
    const normalized = normalizePath(path);
    if (this.#routes.has(normalized)) {
      throw new Error(`HTTP path ${normalized} is already registered`);
    }
    this.#routes.set(normalized, handler);
  }

  public async start(context: Context): Promise<void> {
    if (this.#server !== undefined) {
      throw new Error("service HTTP server is already started");
    }
    const config = this.#config();
    if (config.httpHost.length === 0) {
      throw new Error(`HTTP host is required for service ${config.name}`);
    }
    const server = createServer((request, response) => {
      this.route(request, response);
    });
    this.#server = server;
    try {
      await listen(server, config.httpPort, config.httpHost, context.signal());
    } catch (error: unknown) {
      this.#server = undefined;
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    const server = this.#server;
    if (server === undefined) {
      return;
    }
    this.#server = undefined;
    await close(server, context.signal());
  }

  private route(request: IncomingMessage, response: ServerResponse): void {
    let path: string;
    try {
      path = new URL(request.url ?? "", "http://service.local").pathname;
    } catch {
      response.statusCode = 400;
      response.end("invalid request target");
      return;
    }
    const handler = this.#routes.get(path);
    if (handler === undefined) {
      response.statusCode = 404;
      response.end();
      return;
    }
    try {
      handler(request, response);
    } catch (error: unknown) {
      if (!response.headersSent) response.statusCode = 500;
      if (!response.writableEnded) response.end("internal server error");
      void error;
    }
  }
}

function normalizePath(path: string): string {
  if (path.length === 0) {
    throw new Error("HTTP path must not be empty");
  }
  return path.startsWith("/") ? path : `/${path}`;
}

function listen(server: Server, port: number, host: string, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.reject(abortReason(signal, "HTTP startup cancelled"));
  }
  return new Promise((resolve, reject) => {
    const listening = (): void => {
      cleanup();
      resolve();
    };
    const failed = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const cancelled = (): void => {
      cleanup();
      server.close();
      reject(abortReason(signal, "HTTP startup cancelled"));
    };
    const cleanup = (): void => {
      server.removeListener("listening", listening);
      server.removeListener("error", failed);
      signal.removeEventListener("abort", cancelled);
    };
    server.once("listening", listening);
    server.once("error", failed);
    signal.addEventListener("abort", cancelled, { once: true });
    server.listen(port, host);
  });
}

function close(server: Server, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cancelled = (): void => {
      server.closeAllConnections();
    };
    signal.addEventListener("abort", cancelled, { once: true });
    if (signal.aborted) cancelled();
    server.close((error) => {
      signal.removeEventListener("abort", cancelled);
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

function abortReason(signal: AbortSignal, fallback: string): Error {
  return signal.reason instanceof Error ? signal.reason : new Error(fallback);
}
