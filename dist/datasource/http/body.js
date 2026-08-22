export const DEFAULT_REQUEST_BODY_LIMIT = 1024 * 1024;
export class HttpRequestError extends Error {
    statusCode;
    responseMessage;
    constructor(statusCode, responseMessage, options) {
        super(responseMessage, options);
        this.name = "HttpRequestError";
        this.statusCode = statusCode;
        this.responseMessage = responseMessage;
    }
}
export class InvalidJsonBodyError extends HttpRequestError {
    constructor(cause) {
        const options = cause === undefined ? undefined : { cause };
        super(400, "invalid JSON body", options);
        this.name = "InvalidJsonBodyError";
    }
}
export class RequestBodyTooLargeError extends HttpRequestError {
    limit;
    constructor(limit) {
        super(413, "request body is too large");
        this.name = "RequestBodyTooLargeError";
        this.limit = limit;
    }
}
export async function readRequestBody(request, maxBytes = DEFAULT_REQUEST_BODY_LIMIT) {
    validateBodyLimit(maxBytes);
    rejectKnownOversize(request, maxBytes);
    return new Promise((resolve, reject) => {
        const chunks = [];
        let size = 0;
        const completed = () => {
            cleanup();
            resolve(Buffer.concat(chunks, size));
        };
        const failed = (error) => {
            cleanup();
            reject(error);
        };
        const aborted = () => {
            failed(new Error("HTTP request body was aborted"));
        };
        const received = (chunk) => {
            const bytes = typeof chunk === "string"
                ? Buffer.from(chunk)
                : chunk instanceof Uint8Array
                    ? chunk
                    : undefined;
            if (bytes === undefined) {
                failed(new TypeError("HTTP request emitted a non-byte chunk"));
                request.resume();
                return;
            }
            size += bytes.byteLength;
            if (size > maxBytes) {
                failed(new RequestBodyTooLargeError(maxBytes));
                request.resume();
                return;
            }
            chunks.push(bytes);
        };
        const cleanup = () => {
            request.removeListener("data", received);
            request.removeListener("end", completed);
            request.removeListener("error", failed);
            request.removeListener("aborted", aborted);
        };
        request.on("data", received);
        request.once("end", completed);
        request.once("error", failed);
        request.once("aborted", aborted);
    });
}
export async function readJsonBody(request, decode, maxBytes = DEFAULT_REQUEST_BODY_LIMIT) {
    const bytes = await readRequestBody(request, maxBytes);
    let value;
    try {
        value = JSON.parse(Buffer.from(bytes).toString("utf8"));
    }
    catch (error) {
        throw new InvalidJsonBodyError(error);
    }
    try {
        return decode(value);
    }
    catch (error) {
        if (error instanceof HttpRequestError) {
            throw error;
        }
        throw new InvalidJsonBodyError(error);
    }
}
export function writeRequestError(response, error) {
    if (!(error instanceof HttpRequestError)) {
        return false;
    }
    if (!response.headersSent) {
        response.statusCode = error.statusCode;
        response.setHeader("content-type", "text/plain; charset=utf-8");
        response.setHeader("connection", "close");
    }
    if (!response.writableEnded) {
        response.end(`${error.responseMessage}\n`);
    }
    return true;
}
export function writeJsonResponse(response, statusCode, value) {
    const body = JSON.stringify(value);
    if (!response.headersSent) {
        response.statusCode = statusCode;
        response.setHeader("content-type", "application/json");
    }
    response.end(body);
}
function validateBodyLimit(maxBytes) {
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
        throw new RangeError("HTTP request body limit must be a positive safe integer");
    }
}
function rejectKnownOversize(request, maxBytes) {
    const value = request.headers["content-length"];
    if (value === undefined) {
        return;
    }
    if (!/^\d+$/.test(value)) {
        throw new HttpRequestError(400, "invalid content-length");
    }
    const length = Number(value);
    if (!Number.isSafeInteger(length)) {
        throw new HttpRequestError(400, "invalid content-length");
    }
    if (length > maxBytes) {
        request.resume();
        throw new RequestBodyTooLargeError(maxBytes);
    }
}
//# sourceMappingURL=body.js.map