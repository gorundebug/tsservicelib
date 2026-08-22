import type { IncomingMessage, ServerResponse } from "node:http";
export declare const DEFAULT_REQUEST_BODY_LIMIT: number;
export declare class HttpRequestError extends Error {
    readonly statusCode: number;
    readonly responseMessage: string;
    constructor(statusCode: number, responseMessage: string, options?: ErrorOptions);
}
export declare class InvalidJsonBodyError extends HttpRequestError {
    constructor(cause?: unknown);
}
export declare class RequestBodyTooLargeError extends HttpRequestError {
    readonly limit: number;
    constructor(limit: number);
}
export declare function readRequestBody(request: IncomingMessage, maxBytes?: number): Promise<Uint8Array>;
export declare function readJsonBody<T>(request: IncomingMessage, decode: (value: unknown) => T, maxBytes?: number): Promise<T>;
export declare function writeRequestError(response: ServerResponse, error: unknown): boolean;
export declare function writeJsonResponse(response: ServerResponse, statusCode: number, value: unknown): void;
//# sourceMappingURL=body.d.ts.map