import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import {
  type Context,
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  FunctionCollector,
  InputDataSource,
  MessageContext,
  RotatingMap,
  RuntimeTaskRegistry,
  STREAM_ID_HEADER,
  TRACE_SAMPLING_HEADER,
  applyDataSourceEndpointTracing,
  errorFromUnknown,
  boolAttribute,
  makeStreamContext,
  newStreamId,
  requireHttpDataConnectorConfig,
  requireHttpEndpointConfig,
  spanError,
  stringAttribute,
  type Completion,
  type Consumer,
  type HttpEndpointConfig,
  type HTTPHandler,
  type InputEndpoint,
  type InputEndpointConsumer,
  type RuntimeEnvironment,
  type Span,
  type StreamContext,
  type Tracer,
  type TypedInputStream
} from "../../runtime/index.js";

export type { HTTPHandler } from "../../runtime/index.js";

const PENDING_ROTATION_INTERVAL_MS = 30_000;

export interface HandlerData {
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
}

// ReqT/ResR are canonical transport boundary parameters used by generated
// handlers even though correlation callbacks operate on the graph result R.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export type ResultCallback<HandlerState, ReqT, ResR, T, R, E> = (
  context: MessageContext,
  stream: StreamContext<T, R, E>,
  handlerState: HandlerState,
  value: Readonly<R>,
  data: HandlerData
) => boolean | Promise<boolean>;

export interface ResultContext<HandlerState, ReqT, ResR, T, R, E> {
  setResultCallback(
    messageId: string,
    callback: ResultCallback<HandlerState, ReqT, ResR, T, R, E>
  ): void;
  done(): void;
}

export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
  beginRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    data: HandlerData
  ):
    | { readonly context: MessageContext; readonly state: HandlerState }
    | Promise<{ readonly context: MessageContext; readonly state: HandlerState }>;
  consumeMessage(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    handlerState: HandlerState,
    data: HandlerData,
    resultContext: ResultContext<HandlerState, ReqT, ResR, T, R, E>
  ): Completion;
  getMessageId(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    handlerState: HandlerState,
    value: Readonly<R>
  ): string;
  endRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    error: Error | undefined,
    handlerState: HandlerState,
    data: HandlerData
  ): Completion;
}

class HttpResult<HandlerState, ReqT, ResR, T, R, E> implements ResultContext<
  HandlerState,
  ReqT,
  ResR,
  T,
  R,
  E
> {
  public readonly handlerState: HandlerState;
  public readonly data: HandlerData;
  public readonly span: Span | undefined;
  readonly #callbacks = new Map<string, ResultCallback<HandlerState, ReqT, ResR, T, R, E>>();
  readonly #done: Promise<void>;
  #resolveDone: (() => void) | undefined;
  #doneCalled = false;
  #retiring = false;
  #activeCallbacks = 0;
  #retired: Promise<void> | undefined;
  #resolveRetired: (() => void) | undefined;

  public constructor(handlerState: HandlerState, data: HandlerData, span?: Span) {
    this.handlerState = handlerState;
    this.data = data;
    this.span = span;
    this.#done = new Promise((resolve) => {
      this.#resolveDone = resolve;
    });
  }

  public setResultCallback(
    messageId: string,
    callback: ResultCallback<HandlerState, ReqT, ResR, T, R, E>
  ): void {
    this.#callbacks.set(messageId, callback);
  }

  public done(): void {
    if (this.#doneCalled) {
      return;
    }
    this.#doneCalled = true;
    this.span?.addEvent("done_called");
    this.#resolveDone?.();
    this.#resolveDone = undefined;
  }

  public wait(): Promise<void> {
    return this.#done;
  }

  public callback(
    messageId: string
  ): ResultCallback<HandlerState, ReqT, ResR, T, R, E> | undefined {
    return this.#callbacks.get(messageId);
  }

  public removeCallback(
    messageId: string,
    callback: ResultCallback<HandlerState, ReqT, ResR, T, R, E>
  ): boolean {
    if (this.#callbacks.get(messageId) !== callback) {
      return false;
    }
    return this.#callbacks.delete(messageId);
  }

  public beginCallback(): boolean {
    if (this.#retiring) {
      return false;
    }
    this.#activeCallbacks += 1;
    return true;
  }

  public endCallback(): void {
    this.#activeCallbacks -= 1;
    if (this.#retiring && this.#activeCallbacks === 0) {
      this.#resolveRetired?.();
      this.#resolveRetired = undefined;
    }
  }

  public async retire(): Promise<boolean> {
    this.#retiring = true;
    if (this.#activeCallbacks !== 0) {
      this.#retired ??= new Promise((resolve) => {
        this.#resolveRetired = resolve;
      });
      await this.#retired;
    }
    return this.#doneCalled;
  }
}

class NodeHttpInputEndpoint extends DataSourceEndpoint {
  public readonly method: "GET" | "POST";
  public readonly path: string;
  #consumer: NodeHttpEndpointConsumerContract | undefined;
  readonly #requestHandler: HTTPHandler;

  public constructor(dataSource: NodeHttpDataSource, config: HttpEndpointConfig) {
    super(dataSource, config.id);
    if (config.httpMethodType !== "GET" && config.httpMethodType !== "POST") {
      throw new Error(`no method specified for HTTP endpoint ${config.name}`);
    }
    if (config.path.length === 0) {
      throw new Error(`no path specified for HTTP endpoint ${config.name}`);
    }
    this.method = config.httpMethodType;
    this.path = config.path;
    this.#requestHandler = (request, response) => {
      void this.serve(request, response).catch((error: unknown) => {
        if (!response.headersSent) {
          response.statusCode = 500;
          response.end("internal server error");
        } else if (!response.writableEnded) {
          response.destroy(errorFromUnknown(error));
        }
      });
    };
  }

  public bindConsumer(consumer: NodeHttpEndpointConsumerContract): void {
    if (this.#consumer !== undefined) {
      throw new Error(`consumer already assigned to HTTP endpoint ${this.name}`);
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

  public handler(): HTTPHandler {
    return this.#requestHandler;
  }

  private async serve(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (request.method !== this.method) {
      this.onInvalidHttpMethod(contextFromRequest(request), request.method ?? "");
      response.setHeader("allow", this.method);
      response.statusCode = 405;
      response.end();
      return;
    }
    if (this.#consumer === undefined) {
      response.statusCode = 503;
      response.end("endpoint consumer is not registered");
      return;
    }
    await this.#consumer.serveHttp(request, response);
  }
}

interface NodeHttpEndpointConsumerContract extends InputEndpointConsumer {
  start(context: Context): Promise<void>;
  stop(context: Context): Promise<void>;
  serveHttp(request: IncomingMessage, response: ServerResponse): Promise<void>;
}

export class NodeHttpDataSource extends InputDataSource {
  readonly #routes = new Map<string, NodeHttpInputEndpoint>();
  #server: Server | undefined;
  #started = false;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    requireHttpDataConnectorConfig(this.config());
  }

  public addHttpEndpoint(endpoint: NodeHttpInputEndpoint): void {
    if (this.#routes.has(endpoint.path)) {
      throw new Error(`HTTP path ${endpoint.path} is already registered`);
    }
    this.#routes.set(endpoint.path, endpoint);
    this.addEndpoint(endpoint);
    const config = requireHttpDataConnectorConfig(this.config());
    if (!config.useDedicatedListener) {
      this.runtimeEnvironment().registerHttpHandler(endpoint.path, endpoint.handler());
    }
  }

  public async start(context: Context): Promise<void> {
    if (this.#started) {
      throw new Error(`HTTP data source ${this.name} is already started`);
    }
    this.#started = true;
    try {
      for (const endpoint of this.httpEndpoints()) {
        await endpoint.start(context);
      }
      const config = requireHttpDataConnectorConfig(this.config());
      if (!config.useDedicatedListener) {
        return;
      }
      if (
        config.host === undefined ||
        config.host.length === 0 ||
        config.port === undefined ||
        config.port === 0
      ) {
        throw new Error(`host and port are required for HTTP data connector ${this.name}`);
      }
      const server = createServer((request, response) => {
        this.route(request, response);
      });
      this.#server = server;
      await listen(server, config.port, config.host, context.signal());
    } catch (error: unknown) {
      this.#started = false;
      const server = this.#server;
      this.#server = undefined;
      if (server?.listening === true) {
        try {
          await closeServer(server, context.signal());
        } catch {
          // Preserve the original startup failure after best-effort rollback.
        }
      }
      try {
        await this.stopEndpoints(context);
      } catch {
        // Preserve the original startup failure after best-effort rollback.
      }
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) {
      return;
    }
    this.#started = false;
    const server = this.#server;
    this.#server = undefined;
    try {
      await this.stopEndpoints(context);
    } finally {
      if (server !== undefined) {
        await closeServer(server, context.signal());
      }
    }
  }

  private httpEndpoints(): readonly NodeHttpInputEndpoint[] {
    return [...this.#routes.values()];
  }

  private async stopEndpoints(context: Context): Promise<void> {
    for (const endpoint of this.httpEndpoints()) {
      await endpoint.stop(context);
    }
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
    const endpoint = this.#routes.get(path);
    if (endpoint === undefined) {
      response.statusCode = 404;
      response.end();
      return;
    }
    endpoint.handler()(request, response);
  }
}

class NodeHttpEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>
  implements NodeHttpEndpointConsumerContract, Consumer<T>
{
  readonly #base: DataSourceEndpointConsumer<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #streamContext: StreamContext<T, R, E>;
  readonly #hasResult: boolean;
  readonly #tracer: Tracer | undefined;
  readonly #tasks = new RuntimeTaskRegistry();
  #pending: RotatingMap<string, HttpResult<HandlerState, ReqT, ResR, T, R, E>> | undefined;
  #started = false;
  #stopped = false;

  public constructor(
    endpoint: NodeHttpInputEndpoint,
    stream: TypedInputStream<T, R, E>,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    this.#base = new DataSourceEndpointConsumer(endpoint, stream);
    this.#handler = handler;
    this.#hasResult = stream.resultStream() !== undefined;
    this.#streamContext = makeStreamContext(
      stream,
      stream.resultStream(),
      new FunctionCollector((context, value: T) => this.consume(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    if (this.#hasResult) {
      stream.setResultConsumer({
        consume: (context, value) => this.consumeResult(context, value)
      });
    }
  }

  public endpoint(): InputEndpoint {
    return this.#base.endpoint();
  }

  public stream(): TypedInputStream<T, R, E> {
    return this.#base.stream();
  }

  public consume(context: MessageContext, value: T): Completion {
    return this.#base.consume(context, value);
  }

  public start(context: Context): Promise<void> {
    if (this.#started) {
      return Promise.reject(new Error(`HTTP endpoint ${this.endpoint().name} is already started`));
    }
    if (this.#stopped) {
      return Promise.reject(new Error(`HTTP endpoint ${this.endpoint().name} is stopped`));
    }
    this.#started = true;
    if (!this.#hasResult) {
      return Promise.resolve();
    }
    if (this.#pending !== undefined) {
      return Promise.reject(new Error(`HTTP endpoint ${this.endpoint().name} is already started`));
    }
    this.#pending = new RotatingMap(PENDING_ROTATION_INTERVAL_MS);
    this.#pending.start(context);
    return Promise.resolve();
  }

  public stop(context: Context): Promise<void> {
    if (!this.#started) {
      return Promise.resolve();
    }
    this.#started = false;
    this.#stopped = true;
    this.#tasks.stopAdmission();
    return drainAcceptedTasks(this.#tasks, context).finally(() => {
      this.#pending?.stop(context);
    });
  }

  public serveHttp(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (!this.#started) {
      response.statusCode = 503;
      response.end("HTTP endpoint is not accepting requests");
      return Promise.resolve();
    }
    const cancellation = requestCancellation(request, response);
    return this.#tasks.admit(
      (lifecycleSignal) => this.serveAccepted(request, response, cancellation, lifecycleSignal),
      cancellation.signal
    );
  }

  private async serveAccepted(
    request: IncomingMessage,
    response: ServerResponse,
    cancellation: { readonly signal: AbortSignal; complete(): void },
    lifecycleSignal: AbortSignal
  ): Promise<void> {
    let context = applyDataSourceEndpointTracing(
      contextFromRequest(request, lifecycleSignal),
      this.stream().runtimeEnvironment(),
      this.endpoint().id
    );
    const data: HandlerData = { request, response };
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "http.input", [
        stringAttribute("stream", this.stream().name),
        stringAttribute("endpoint", this.endpoint().name),
        stringAttribute("method", request.method ?? ""),
        stringAttribute("path", requestPath(request))
      ]);
      context = started.context;
      span = started.span;
    }
    let state: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#streamContext, data);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      try {
        cancellation.complete();
      } finally {
        span?.end();
      }
      return;
    }
    span?.addEvent("begin_request");

    const requestStarted = this.endpoint().onRequestStart(context);

    let streamId = context.streamId();
    if (streamId === undefined) {
      streamId = newStreamId();
      context = context.withStreamId(streamId);
    }
    span?.setAttributes([
      stringAttribute("stream_id", streamId),
      boolAttribute("has_result", this.#hasResult)
    ]);
    const result = new HttpResult<HandlerState, ReqT, ResR, T, R, E>(state, data, span);
    let pendingAdded = false;
    let requestError: Error | undefined;
    let resultWaitFailed = false;
    try {
      if (this.#hasResult) {
        this.pending().set(streamId, result);
        this.endpoint().onPendingAdd(context, streamId);
        pendingAdded = true;
      }
      try {
        await this.#handler.consumeMessage(context, this.#streamContext, state, data, result);
      } catch (error: unknown) {
        const failure = errorFromUnknown(error);
        span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
        throw failure;
      }
      span?.addEvent("consume_message");
      if (this.#hasResult) {
        const waitFailure = await waitForDoneOrCancellation(result, context.signal());
        if (waitFailure !== undefined) {
          resultWaitFailed = true;
          throw waitFailure;
        }
        span?.addEvent("done_received");
      }
    } catch (error: unknown) {
      requestError = errorFromUnknown(error);
    } finally {
      if (pendingAdded) {
        const resultCompleted = await result.retire();
        if (resultWaitFailed && resultCompleted) requestError = undefined;
        this.pending().pop(streamId);
        this.endpoint().onPendingRemove(context, streamId);
      }
      if (requestError !== undefined) {
        spanError(span, requestError);
        if (context.cancelled()) {
          span?.addEvent("context_cancelled", [stringAttribute("error", requestError.message)]);
        }
      }
      try {
        await this.#handler.endRequest(context, this.#streamContext, requestError, state, data);
      } finally {
        try {
          this.endpoint().onRequestEnd(context, requestStarted, requestError);
        } finally {
          try {
            cancellation.complete();
          } finally {
            span?.end();
          }
        }
      }
    }
  }

  private async consumeResult(context: MessageContext, value: R): Promise<void> {
    const streamId = context.streamId();
    if (streamId === undefined) {
      this.endpoint().onMissingStreamId(context);
      return;
    }
    if (this.#pending === undefined) {
      this.endpoint().onLateResult(context, streamId);
      return;
    }
    const [result, found] = this.#pending.get(streamId);
    if (!found || result?.beginCallback() !== true) {
      this.endpoint().onLateResult(context, streamId);
      return;
    }
    try {
      const messageId = this.#handler.getMessageId(
        context,
        this.#streamContext,
        result.handlerState,
        value
      );
      const callback = result.callback(messageId);
      if (callback === undefined) {
        this.endpoint().onUnknownMessageId(context, streamId, messageId);
        result.span?.addEvent("unknown_message_id", [stringAttribute("message_id", messageId)]);
        return;
      }
      if (await callback(context, this.#streamContext, result.handlerState, value, result.data)) {
        if (!result.removeCallback(messageId, callback)) {
          this.endpoint().onDuplicateMessageId(context, streamId, messageId);
          result.span?.addEvent("duplicate_message_id", [stringAttribute("message_id", messageId)]);
        }
      }
      result.span?.addEvent("result_consumed", [stringAttribute("message_id", messageId)]);
    } finally {
      result.endCallback();
    }
  }

  private pending(): RotatingMap<string, HttpResult<HandlerState, ReqT, ResR, T, R, E>> {
    if (this.#pending === undefined) {
      throw new Error(`HTTP endpoint ${this.endpoint().name} is not started`);
    }
    return this.#pending;
  }
}

async function drainAcceptedTasks(tasks: RuntimeTaskRegistry, context: Context): Promise<void> {
  try {
    await tasks.drain(context.remainingMs());
  } catch (error: unknown) {
    tasks.cancel(context.signal().reason ?? error);
    await tasks.drain();
  }
}

export function makeNodeHttpEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): readonly [consumer: Consumer<T>, handler: HTTPHandler] {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = requireHttpEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
  if (dataSource.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new NodeHttpInputEndpoint(dataSource, endpointConfig);
  const consumer = new NodeHttpEndpointConsumer(endpoint, stream, handler);
  endpoint.bindConsumer(consumer);
  dataSource.addHttpEndpoint(endpoint);
  return [consumer, endpoint.handler()];
}

function getOrCreateDataSource(
  connectorId: number,
  environment: RuntimeEnvironment
): NodeHttpDataSource {
  const existing = environment.dataSourceById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof NodeHttpDataSource)) {
      throw new Error(`data source ${String(connectorId)} is not a Node HTTP data source`);
    }
    return existing;
  }
  const dataSource = new NodeHttpDataSource(connectorId, environment);
  environment.addDataSource(dataSource);
  return dataSource;
}

function contextFromRequest(request: IncomingMessage, signal?: AbortSignal): MessageContext {
  const metadata = new Map<string, string>();
  for (const name of [
    STREAM_ID_HEADER,
    TRACE_SAMPLING_HEADER,
    "traceparent",
    "tracestate",
    "baggage"
  ]) {
    const value = request.headers[name];
    const first = Array.isArray(value) ? value[0] : value;
    if (first !== undefined) {
      metadata.set(name, first);
    }
  }
  return new MessageContext(signal).withMetadata(metadata);
}

function requestPath(request: IncomingMessage): string {
  try {
    return new URL(request.url ?? "", "http://service.local").pathname;
  } catch {
    return "";
  }
}

function requestCancellation(
  request: IncomingMessage,
  response: ServerResponse
): { readonly signal: AbortSignal; complete(): void } {
  const controller = new AbortController();
  const abort = (): void => {
    if (!response.writableEnded) {
      controller.abort(new Error("HTTP peer disconnected"));
    }
  };
  request.once("aborted", abort);
  response.once("close", abort);
  if (request.destroyed && !request.complete) {
    abort();
  }
  return {
    signal: controller.signal,
    complete(): void {
      // Go's net/http cancels Request.Context when ServeHTTP returns. Preserve
      // that lifecycle boundary so detached graph branches (for example the
      // soft-deadline branch) are cancelled as soon as the response is done.
      if (!controller.signal.aborted) {
        controller.abort(new Error("HTTP request completed"));
      }
      request.removeListener("aborted", abort);
      response.removeListener("close", abort);
    }
  };
}

function waitForDoneOrCancellation<HandlerState, ReqT, ResR, T, R, E>(
  result: HttpResult<HandlerState, ReqT, ResR, T, R, E>,
  signal: AbortSignal
): Promise<Error | undefined> {
  if (signal.aborted) {
    return Promise.resolve(abortReason(signal, "HTTP request cancelled"));
  }
  return new Promise((resolve) => {
    const cancelled = (): void => {
      resolve(abortReason(signal, "HTTP request cancelled"));
    };
    signal.addEventListener("abort", cancelled, { once: true });
    void result.wait().then(
      () => {
        signal.removeEventListener("abort", cancelled);
        resolve(undefined);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", cancelled);
        resolve(errorFromUnknown(error));
      }
    );
  });
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

function closeServer(server: Server, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cancelled = (): void => {
      server.closeAllConnections();
    };
    signal.addEventListener("abort", cancelled, { once: true });
    if (signal.aborted) {
      cancelled();
    }
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
  return signal.reason === undefined ? new Error(fallback) : errorFromUnknown(signal.reason);
}
