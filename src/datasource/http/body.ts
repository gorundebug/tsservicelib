import type { IncomingMessage, ServerResponse } from "node:http";

export const DEFAULT_REQUEST_BODY_LIMIT = 1024 * 1024;

export class HttpRequestError extends Error {
  public readonly statusCode: number;
  public readonly responseMessage: string;

  public constructor(statusCode: number, responseMessage: string, options?: ErrorOptions) {
    super(responseMessage, options);
    this.name = "HttpRequestError";
    this.statusCode = statusCode;
    this.responseMessage = responseMessage;
  }
}

export class InvalidJsonBodyError extends HttpRequestError {
  public constructor(cause?: unknown) {
    const options = cause === undefined ? undefined : { cause };
    super(400, "invalid JSON body", options);
    this.name = "InvalidJsonBodyError";
  }
}

export class RequestBodyTooLargeError extends HttpRequestError {
  public readonly limit: number;

  public constructor(limit: number) {
    super(413, "request body is too large");
    this.name = "RequestBodyTooLargeError";
    this.limit = limit;
  }
}

export async function readRequestBody(
  request: IncomingMessage,
  maxBytes: number = DEFAULT_REQUEST_BODY_LIMIT
): Promise<Uint8Array> {
  validateBodyLimit(maxBytes);
  rejectKnownOversize(request, maxBytes);
  return new Promise((resolve, reject) => {
    const chunks: Uint8Array[] = [];
    let size = 0;
    const completed = (): void => {
      cleanup();
      resolve(Buffer.concat(chunks, size));
    };
    const failed = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const aborted = (): void => {
      failed(new Error("HTTP request body was aborted"));
    };
    const received = (chunk: unknown): void => {
      const bytes =
        typeof chunk === "string"
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
    const cleanup = (): void => {
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

export async function readJsonBody<T>(
  request: IncomingMessage,
  decode: (value: unknown) => T,
  maxBytes: number = DEFAULT_REQUEST_BODY_LIMIT
): Promise<T> {
  const bytes = await readRequestBody(request, maxBytes);
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(bytes).toString("utf8")) as unknown;
  } catch (error: unknown) {
    throw new InvalidJsonBodyError(error);
  }
  try {
    return decode(value);
  } catch (error: unknown) {
    if (error instanceof HttpRequestError) {
      throw error;
    }
    throw new InvalidJsonBodyError(error);
  }
}

export function writeRequestError(response: ServerResponse, error: unknown): boolean {
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

export function writeJsonResponse(
  response: ServerResponse,
  statusCode: number,
  value: unknown
): void {
  const body = JSON.stringify(value);
  if (!response.headersSent) {
    response.statusCode = statusCode;
    response.setHeader("content-type", "application/json");
  }
  response.end(body);
}

function validateBodyLimit(maxBytes: number): void {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new RangeError("HTTP request body limit must be a positive safe integer");
  }
}

function rejectKnownOversize(request: IncomingMessage, maxBytes: number): void {
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
