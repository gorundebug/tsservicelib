import {
  Agent as HttpAgent,
  request as httpRequest,
  type IncomingHttpHeaders,
  type IncomingMessage
} from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { Readable } from "node:stream";

import {
  DataSinkEndpoint,
  DataSinkEndpointConsumerWithResult,
  FunctionCollector,
  type MessageContext,
  OutputDataSink,
  RuntimeTaskRegistry,
  SinkStreamContext,
  int64Attribute,
  errorFromUnknown,
  newStreamId,
  requireHttpDataConnectorConfig,
  requireHttpEndpointConfig,
  spanError,
  stringAttribute,
  type Completion,
  type Consumer,
  type Context,
  type HttpDataConnectorConfig,
  type HttpEndpointConfig,
  type OutputEndpointConsumer,
  type RuntimeEnvironment,
  type SinkEndpoint,
  type Span,
  type Tracer,
  type TypedSinkStreamWithResult
} from "../../runtime/index.js";

export type RequestBody = string | Uint8Array | Readable | undefined;

export class Request {
  public readonly context: MessageContext;
  public readonly method: string;
  public readonly url: URL;
  public readonly headers: Headers = new Headers();
  public readonly body: RequestBody;

  public constructor(
    context: MessageContext,
    method: string,
    url: string | URL,
    body?: RequestBody
  ) {
    this.context = context;
    this.method = method;
    this.url = typeof url === "string" ? new URL(url) : url;
    this.body = body;
  }
}

export class Requester {
  #request: Request | undefined;

  public newRequest(
    context: MessageContext,
    method: string,
    url: string | URL,
    body?: RequestBody
  ): Request {
    const request = new Request(context, method, url, body);
    this.#request = request;
    return request;
  }

  public request(): Request | undefined {
    return this.#request;
  }
}

export class Response {
  public readonly statusCode: number;
  public readonly status: string;
  public readonly headers: IncomingHttpHeaders;
  public readonly body: IncomingMessage;

  public constructor(body: IncomingMessage) {
    this.body = body;
    this.statusCode = body.statusCode ?? 0;
    this.status = `${String(this.statusCode)}${body.statusMessage === undefined ? "" : ` ${body.statusMessage}`}`;
    this.headers = body.headers;
  }

  public async read(maxBytes: number = Number.MAX_SAFE_INTEGER): Promise<Uint8Array> {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
      throw new RangeError("HTTP response body limit must be a non-negative safe integer");
    }
    const chunks: Uint8Array[] = [];
    let size = 0;
    for await (const chunk of this.body) {
      const value: unknown = chunk;
      const bytes =
        typeof value === "string"
          ? Buffer.from(value)
          : value instanceof Uint8Array
            ? value
            : undefined;
      if (bytes === undefined) {
        throw new TypeError("HTTP response emitted a non-byte chunk");
      }
      size += bytes.byteLength;
      if (size > maxBytes) {
        throw new ResponseBodyTooLargeError(maxBytes);
      }
      chunks.push(bytes);
    }
    return Buffer.concat(chunks, size);
  }

  public async text(maxBytes?: number): Promise<string> {
    return Buffer.from(await this.read(maxBytes)).toString("utf8");
  }

  public async close(): Promise<void> {
    if (this.body.complete || this.body.destroyed) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const completed = (): void => {
        cleanup();
        resolve();
      };
      const failed = (error: Error): void => {
        cleanup();
        reject(error);
      };
      const cleanup = (): void => {
        this.body.removeListener("end", completed);
        this.body.removeListener("close", completed);
        this.body.removeListener("error", failed);
      };
      this.body.once("end", completed);
      this.body.once("close", completed);
      this.body.once("error", failed);
      this.body.resume();
    });
  }
}

export class ResponseBodyTooLargeError extends Error {
  public readonly limit: number;

  public constructor(limit: number) {
    super(`HTTP response body exceeds ${String(limit)} bytes`);
    this.name = "ResponseBodyTooLargeError";
    this.limit = limit;
  }
}

export interface Client {
  do(request: Request): Promise<Response>;
  close(context: Context): Promise<void>;
}

export interface NodeHttpClientOptions {
  readonly maxSockets?: number;
  readonly maxFreeSockets?: number;
}

export class NodeHttpClient implements Client {
  readonly #httpAgent: HttpAgent;
  readonly #httpsAgent: HttpsAgent;
  #closed = false;

  public constructor(options: NodeHttpClientOptions = {}) {
    const agentOptions = {
      keepAlive: true,
      maxSockets: options.maxSockets ?? Infinity,
      maxFreeSockets: options.maxFreeSockets ?? 256
    };
    this.#httpAgent = new HttpAgent(agentOptions);
    this.#httpsAgent = new HttpsAgent(agentOptions);
  }

  public do(request: Request): Promise<Response> {
    if (this.#closed) {
      return Promise.reject(new Error("HTTP client is closed"));
    }
    if (request.context.cancelled()) {
      return Promise.reject(contextError(request.context, "HTTP request context is cancelled"));
    }
    const protocol = request.url.protocol;
    if (protocol !== "http:" && protocol !== "https:") {
      return Promise.reject(new Error(`unsupported HTTP protocol ${protocol}`));
    }
    const transport = protocol === "https:" ? httpsRequest : httpRequest;
    const agent = protocol === "https:" ? this.#httpsAgent : this.#httpAgent;
    return new Promise((resolve, reject) => {
      const outgoing = transport(
        request.url,
        {
          method: request.method,
          headers: Object.fromEntries(request.headers.entries()),
          agent,
          signal: request.context.signal()
        },
        (incoming) => {
          resolve(new Response(incoming));
        }
      );
      outgoing.once("error", (error) => {
        reject(error);
      });
      const remaining = request.context.remainingMs();
      if (remaining !== undefined) {
        outgoing.setTimeout(Math.max(1, Math.ceil(remaining)), () => {
          outgoing.destroy(new Error("HTTP request deadline exceeded"));
        });
      }
      const body = request.body;
      if (body instanceof Readable) {
        body.once("error", (error) => {
          outgoing.destroy(error);
        });
        body.pipe(outgoing);
      } else {
        outgoing.end(body);
      }
    });
  }

  public close(context: Context): Promise<void> {
    void context;
    if (!this.#closed) {
      this.#closed = true;
      this.#httpAgent.destroy();
      this.#httpsAgent.destroy();
    }
    return Promise.resolve();
  }
}

// ReqT/ResR are retained for generated business-handler type parity. The raw
// wire request and response are represented by Requester and Response.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
  beginRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>
  ):
    | { readonly context: MessageContext; readonly state: HandlerState }
    | Promise<{ readonly context: MessageContext; readonly state: HandlerState }>;
  consumeMessage(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    handlerState: HandlerState,
    value: Readonly<T>,
    requester: Requester
  ): Completion;
  handleResponse(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    handlerState: HandlerState,
    response: Readonly<Response>
  ): Completion;
  endRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    error: Error | undefined,
    handlerState: HandlerState
  ): Completion;
}

export class StreamContext<T, R, E> extends SinkStreamContext<T, R, E> {
  readonly #environment: RuntimeEnvironment;
  readonly #endpointId: number;
  readonly #connectorId: number;

  public constructor(stream: TypedSinkStreamWithResult<T, R, E>) {
    const environment = stream.runtimeEnvironment();
    const endpoint = requireHttpEndpointConfig(
      environment.runtimeConfig().endpointById(stream.endpointId())
    );
    super(
      stream,
      environment.log(),
      new FunctionCollector((context, value: R) => stream.consumeResult(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
    this.#environment = environment;
    this.#endpointId = endpoint.id;
    this.#connectorId = endpoint.idDataConnector;
  }

  public get endpointConfig(): HttpEndpointConfig {
    return requireHttpEndpointConfig(
      this.#environment.runtimeConfig().endpointById(this.#endpointId)
    );
  }

  public get dataConnectorConfig(): HttpDataConnectorConfig {
    return requireHttpDataConnectorConfig(
      this.#environment.runtimeConfig().dataConnectorById(this.#connectorId)
    );
  }
}

class NodeHttpSinkEndpoint extends DataSinkEndpoint {
  #consumer: NodeHttpSinkEndpointConsumerContract | undefined;

  public bindConsumer(consumer: NodeHttpSinkEndpointConsumerContract): void {
    if (this.#consumer !== undefined) {
      throw new Error(`consumer already assigned to HTTP sink endpoint ${this.name}`);
    }
    this.#consumer = consumer;
    this.addEndpointConsumer(consumer);
  }

  public start(context: Context): Promise<void> {
    return this.#consumer?.start(context) ?? Promise.resolve();
  }

  public stop(context: Context): Promise<void> {
    return this.#consumer?.stop(context) ?? Promise.resolve();
  }
}

interface NodeHttpSinkEndpointConsumerContract extends OutputEndpointConsumer {
  start(context: Context): Promise<void>;
  stop(context: Context): Promise<void>;
}

export class NodeHttpDataSink extends OutputDataSink {
  readonly #client: Client;
  #started = false;

  public constructor(connectorId: number, environment: RuntimeEnvironment, client: Client) {
    super(connectorId, environment);
    requireHttpDataConnectorConfig(this.config());
    this.#client = client;
  }

  public client(): Client {
    return this.#client;
  }

  public async start(context: Context): Promise<void> {
    if (this.#started) {
      throw new Error(`HTTP data sink ${this.name} is already started`);
    }
    this.#started = true;
    try {
      for (const endpoint of this.httpEndpoints()) {
        await endpoint.start(context);
      }
    } catch (error: unknown) {
      this.#started = false;
      await this.stopEndpoints(context);
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) {
      return;
    }
    this.#started = false;
    try {
      await this.stopEndpoints(context);
    } finally {
      await this.#client.close(context);
    }
  }

  private httpEndpoints(): readonly NodeHttpSinkEndpoint[] {
    return this.endpoints().map((endpoint) => {
      if (!(endpoint instanceof NodeHttpSinkEndpoint)) {
        throw new Error(`sink endpoint ${endpoint.name} is not a Node HTTP endpoint`);
      }
      return endpoint;
    });
  }

  private async stopEndpoints(context: Context): Promise<void> {
    await Promise.all(this.httpEndpoints().map(async (endpoint) => endpoint.stop(context)));
  }
}

class NodeHttpSinkEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>
  implements NodeHttpSinkEndpointConsumerContract, Consumer<T>
{
  readonly #base: DataSinkEndpointConsumerWithResult<T, R, E>;
  readonly #streamContext: StreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #client: Client;
  readonly #tracer: Tracer | undefined;
  readonly #tasks = new RuntimeTaskRegistry();
  #started = false;
  #stopped = false;

  public constructor(
    endpoint: NodeHttpSinkEndpoint,
    stream: TypedSinkStreamWithResult<T, R, E>,
    client: Client,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
    this.#streamContext = new StreamContext(stream);
    this.#client = client;
    this.#handler = handler;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public start(context: Context): Promise<void> {
    void context;
    if (this.#started) {
      return Promise.reject(
        new Error(`HTTP sink endpoint ${this.endpoint().name} is already started`)
      );
    }
    if (this.#stopped) {
      return Promise.reject(new Error(`HTTP sink endpoint ${this.endpoint().name} is stopped`));
    }
    this.#started = true;
    return Promise.resolve();
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) {
      return;
    }
    this.#started = false;
    this.#stopped = true;
    this.#tasks.stopAdmission();
    await drainAcceptedTasks(this.#tasks, context);
  }

  public consume(context: MessageContext, value: T): Promise<void> {
    if (!this.#started) {
      return Promise.resolve();
    }
    return this.#tasks.admit(
      async (lifecycleSignal) =>
        this.consumeOnce(context.withExternalCancellation(lifecycleSignal), value),
      context.signal()
    );
  }

  private async consumeOnce(context: MessageContext, value: T): Promise<void> {
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "http.output", [
        stringAttribute("stream", this.#base.stream().name),
        stringAttribute("endpoint", this.endpoint().name)
      ]);
      context = started.context;
      span = started.span;
    }
    let handlerContext: MessageContext;
    let handlerState: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#streamContext);
      handlerContext = started.context;
      handlerState = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");

    const requestStarted = this.endpoint().onRequestStart(handlerContext);
    let requestError: Error | undefined;
    let response: Response | undefined;
    let errorEvent = "consume_message.error";
    try {
      const requester = new Requester();
      await this.#handler.consumeMessage(
        handlerContext,
        this.#streamContext,
        handlerState,
        value,
        requester
      );
      span?.addEvent("consume_message");
      const request = requester.request();
      if (request === undefined) {
        errorEvent = "no_request.error";
        throw new Error(`no HTTP request set by handler for sink endpoint ${this.endpoint().name}`);
      }
      const requestContext = request.context.withStreamId(newStreamId());
      const outgoingRequest = new Request(
        requestContext,
        request.method,
        request.url,
        request.body
      );
      for (const [name, value] of request.headers) {
        outgoingRequest.headers.set(name, value);
      }
      for (const [name, metadata] of requestContext.transportMetadata()) {
        outgoingRequest.headers.set(name, metadata);
      }
      errorEvent = "http_call.error";
      response = await this.#client.do(outgoingRequest);
      span?.addEvent("http_call", [int64Attribute("status_code", BigInt(response.statusCode))]);
      errorEvent = "handle_response.error";
      await this.#handler.handleResponse(
        handlerContext,
        this.#streamContext,
        handlerState,
        response
      );
      span?.addEvent("handle_response");
    } catch (error: unknown) {
      requestError = errorFromUnknown(error);
      spanError(span, requestError);
      span?.addEvent(errorEvent, [stringAttribute("error", requestError.message)]);
    } finally {
      if (response !== undefined) {
        try {
          await response.close();
        } catch (error: unknown) {
          requestError ??= errorFromUnknown(error);
        }
      }
      try {
        await this.#handler.endRequest(
          handlerContext,
          this.#streamContext,
          requestError,
          handlerState
        );
      } finally {
        try {
          this.endpoint().onRequestEnd(handlerContext, requestStarted, requestError);
        } finally {
          span?.end();
        }
      }
    }
  }
}

export function makeNodeHttpEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  client: Client,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = requireHttpEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, client);
  if (dataSink.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new NodeHttpSinkEndpoint(dataSink, endpointConfig.id);
  const consumer = new NodeHttpSinkEndpointConsumer(endpoint, stream, client, handler);
  endpoint.bindConsumer(consumer);
  dataSink.addEndpoint(endpoint);
  stream.setSinkConsumer(consumer);
  return consumer;
}

function getOrCreateDataSink(
  connectorId: number,
  environment: RuntimeEnvironment,
  client: Client
): NodeHttpDataSink {
  const existing = environment.dataSinkById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof NodeHttpDataSink)) {
      throw new Error(`data sink ${String(connectorId)} is not a Node HTTP data sink`);
    }
    if (existing.client() !== client) {
      throw new Error(`HTTP data sink ${existing.name} already uses a different client`);
    }
    return existing;
  }
  const dataSink = new NodeHttpDataSink(connectorId, environment, client);
  environment.addDataSink(dataSink);
  return dataSink;
}

function contextError(context: Context, fallback: string): Error {
  return context.signal().reason === undefined
    ? new Error(fallback)
    : errorFromUnknown(context.signal().reason);
}

async function drainAcceptedTasks(tasks: RuntimeTaskRegistry, context: Context): Promise<void> {
  try {
    await tasks.drain(context.remainingMs());
  } catch (error: unknown) {
    tasks.cancel(context.signal().reason ?? error);
    await tasks.drain();
  }
}
