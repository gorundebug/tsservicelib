import { type IncomingMessage, type ServerResponse } from "node:http";
import type { ServiceConfig } from "./config/index.js";
import type { Context } from "./context.js";
import type { Lifecycle } from "./lifecycle.js";
export type HTTPHandler = (request: IncomingMessage, response: ServerResponse) => void;
/** The service-wide HTTP listener shared by non-dedicated HTTP connectors. */
export declare class ServiceHTTPServer implements Lifecycle {
    #private;
    constructor(config: () => ServiceConfig);
    register(path: string, handler: HTTPHandler): void;
    start(context: Context): Promise<void>;
    stop(context: Context): Promise<void>;
    private route;
}
//# sourceMappingURL=service-http-server.d.ts.map