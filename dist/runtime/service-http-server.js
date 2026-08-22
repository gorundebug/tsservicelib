import { createServer } from "node:http";
/** The service-wide HTTP listener shared by non-dedicated HTTP connectors. */
export class ServiceHTTPServer {
    #config;
    #routes = new Map();
    #server;
    constructor(config) {
        this.#config = config;
    }
    register(path, handler) {
        const normalized = normalizePath(path);
        if (this.#routes.has(normalized)) {
            throw new Error(`HTTP path ${normalized} is already registered`);
        }
        this.#routes.set(normalized, handler);
    }
    async start(context) {
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
        }
        catch (error) {
            this.#server = undefined;
            throw error;
        }
    }
    async stop(context) {
        const server = this.#server;
        if (server === undefined) {
            return;
        }
        this.#server = undefined;
        await close(server, context.signal());
    }
    route(request, response) {
        let path;
        try {
            path = new URL(request.url ?? "", "http://service.local").pathname;
        }
        catch {
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
        }
        catch (error) {
            if (!response.headersSent)
                response.statusCode = 500;
            if (!response.writableEnded)
                response.end("internal server error");
            void error;
        }
    }
}
function normalizePath(path) {
    if (path.length === 0) {
        throw new Error("HTTP path must not be empty");
    }
    return path.startsWith("/") ? path : `/${path}`;
}
function listen(server, port, host, signal) {
    if (signal.aborted) {
        return Promise.reject(abortReason(signal, "HTTP startup cancelled"));
    }
    return new Promise((resolve, reject) => {
        const listening = () => {
            cleanup();
            resolve();
        };
        const failed = (error) => {
            cleanup();
            reject(error);
        };
        const cancelled = () => {
            cleanup();
            server.close();
            reject(abortReason(signal, "HTTP startup cancelled"));
        };
        const cleanup = () => {
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
function close(server, signal) {
    return new Promise((resolve, reject) => {
        const cancelled = () => {
            server.closeAllConnections();
        };
        signal.addEventListener("abort", cancelled, { once: true });
        if (signal.aborted)
            cancelled();
        server.close((error) => {
            signal.removeEventListener("abort", cancelled);
            if (error === undefined) {
                resolve();
            }
            else {
                reject(error);
            }
        });
    });
}
function abortReason(signal, fallback) {
    return signal.reason instanceof Error ? signal.reason : new Error(fallback);
}
//# sourceMappingURL=service-http-server.js.map