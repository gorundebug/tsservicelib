import {
  Server,
  ServerCredentials,
  type handleBidiStreamingCall,
  type handleClientStreamingCall,
  type handleServerStreamingCall,
  type ServerDuplexStream,
  type ServerReadableStream,
  type handleUnaryCall,
  type Metadata,
  type ServerUnaryCall,
  type ServerWritableStream,
  type ServiceDefinition,
  type UntypedHandleCall
} from "@grpc/grpc-js";
import {
  create,
  fromBinary,
  toBinary,
  type DescMessage,
  type DescMethod,
  type DescService,
  type MessageShape
} from "@bufbuild/protobuf";

import {
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  FunctionCollector,
  InputDataSource,
  Context,
  MessageContext,
  STREAM_ID_HEADER,
  TRACE_SAMPLING_HEADER,
  errorFromUnknown,
  err,
  boolAttribute,
  int64Attribute,
  makeStreamContext,
  newStreamId,
  requireGrpcDataConnectorConfig,
  requireGrpcEndpointConfig,
  spanError,
  str,
  stringAttribute,
  type Completion,
  type Consumer,
  type InputEndpointConsumer,
  type InputEndpoint,
  type RuntimeEnvironment,
  type Span,
  type StreamContext,
  type Tracer,
  type TypedInputStream
} from "../../runtime/index.js";

interface GrpcServerCallContext {
  readonly metadata: Metadata;
  getDeadline(): Date | number;
  once(event: "cancelled", listener: () => void): unknown;
}

export interface Sender<ResR> {
  send(context: MessageContext, value: ResR): Completion;
}

export type ResultCallback<HandlerState, T, ResR, R, E> = (
  context: MessageContext,
  stream: StreamContext<T, R, E>,
  state: HandlerState,
  value: Readonly<R>,
  sender: Sender<ResR>
) => boolean | Promise<boolean>;

export interface ResultContext<HandlerState, T, ResR, R, E> {
  setResultCallback(messageId: string, callback: ResultCallback<HandlerState, T, ResR, R, E>): void;
  done(): void;
}

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
    state: HandlerState,
    request: Readonly<ReqT>,
    result: ResultContext<HandlerState, T, ResR, R, E>,
    sender: Sender<ResR>
  ): Completion;
  getMessageId(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    state: HandlerState,
    value: Readonly<R>
  ): string;
  eof(context: MessageContext, stream: StreamContext<T, R, E>, state: HandlerState): Completion;
  endRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    error: Error | undefined,
    state: HandlerState
  ): Completion;
}

class GrpcJsDataSource extends InputDataSource {
  readonly #services = new Map<DescService, Map<string, UntypedHandleCall>>();
  #server: Server | undefined;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    requireGrpcDataConnectorConfig(this.config());
  }

  public add(service: DescService, method: DescMethod, handler: UntypedHandleCall): void {
    let methods = this.#services.get(service);
    if (methods === undefined) {
      methods = new Map();
      this.#services.set(service, methods);
    }
    if (methods.has(method.localName)) throw new Error(`gRPC method ${method.name} already bound`);
    methods.set(method.localName, handler);
  }

  public async start(context: Context): Promise<void> {
    void context;
    if (this.#server !== undefined)
      throw new Error(`gRPC data source ${this.name} already started`);
    const server = new Server();
    for (const [service, handlers] of this.#services) {
      server.addService(serviceDefinition(service), Object.fromEntries(handlers));
    }
    const config = this.runtimeEnvironment().serviceConfig();
    await new Promise<void>((resolve, reject) => {
      server.bindAsync(
        `${config.grpcHost}:${String(config.grpcPort)}`,
        ServerCredentials.createInsecure(),
        (error) => {
          if (error === null) resolve();
          else reject(error);
        }
      );
    });
    this.#server = server;
  }

  public async stop(context: Context): Promise<void> {
    const server = this.#server;
    this.#server = undefined;
    if (server === undefined) return;
    await new Promise<void>((resolve) => {
      const timeout = context.remainingMs();
      const timer =
        timeout === undefined
          ? undefined
          : setTimeout(() => {
              server.forceShutdown();
              resolve();
            }, timeout);
      server.tryShutdown(() => {
        if (timer !== undefined) clearTimeout(timer);
        resolve();
      });
    });
  }
}

class RequestResult<HandlerState, T, ResR, R, E> implements ResultContext<
  HandlerState,
  T,
  ResR,
  R,
  E
> {
  readonly #callbacks = new Map<string, ResultCallback<HandlerState, T, ResR, R, E>>();
  readonly #span: Span | undefined;
  readonly #recordDone: boolean;
  #done: Promise<void> | undefined;
  #resolve: (() => void) | undefined;
  #completed = false;
  #retiring = false;
  #activeCallbacks = 0;
  #retired: Promise<void> | undefined;
  #resolveRetired: (() => void) | undefined;

  public constructor(span: Span | undefined, recordDone: boolean) {
    this.#span = span;
    this.#recordDone = recordDone;
  }
  public setResultCallback(
    messageId: string,
    callback: ResultCallback<HandlerState, T, ResR, R, E>
  ): void {
    this.#callbacks.set(messageId, callback);
  }
  public callback(messageId: string): ResultCallback<HandlerState, T, ResR, R, E> | undefined {
    return this.#callbacks.get(messageId);
  }
  public remove(messageId: string, callback: ResultCallback<HandlerState, T, ResR, R, E>): boolean {
    if (this.#callbacks.get(messageId) !== callback) return false;
    return this.#callbacks.delete(messageId);
  }
  public done(): void {
    if (this.#completed) return;
    this.#completed = true;
    if (this.#recordDone) this.#span?.addEvent("done_called");
    this.#resolve?.();
    this.#resolve = undefined;
  }
  public wait(): Promise<void> {
    if (this.#completed) return Promise.resolve();
    this.#done ??= new Promise((resolve) => {
      this.#resolve = resolve;
    });
    return this.#done;
  }
  public completed(): boolean {
    return this.#completed;
  }
  public span(): Span | undefined {
    return this.#span;
  }
  public beginCallback(): boolean {
    if (this.#retiring) return false;
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
    return this.#completed;
  }
}

class UnarySender<ResR> implements Sender<ResR> {
  readonly #send: (value: ResR) => void;
  readonly #span: Span | undefined;
  readonly #rejectDuplicate: boolean;
  #sent = false;
  public constructor(send: (value: ResR) => void, span: Span | undefined, rejectDuplicate = true) {
    this.#send = send;
    this.#span = span;
    this.#rejectDuplicate = rejectDuplicate;
  }
  public send(_context: MessageContext, value: ResR): void {
    if (this.#sent) {
      if (!this.#rejectDuplicate) return;
      const error = new Error("unary gRPC response already sent");
      spanError(this.#span, error);
      this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
      throw error;
    }
    this.#sent = true;
    this.#span?.addEvent("send");
    this.#send(value);
  }
}

class StreamingSender<ResR> implements Sender<ResR> {
  readonly #write: (value: ResR, callback: (error?: Error | null) => void) => boolean;
  #tail = Promise.resolve();
  #active = true;
  readonly #span: Span | undefined;

  public constructor(
    write: (value: ResR, callback: (error?: Error | null) => void) => boolean,
    span: Span | undefined
  ) {
    this.#write = write;
    this.#span = span;
  }

  public send(_context: MessageContext, value: ResR): Promise<void> {
    if (!this.#active) {
      const error = new Error("stream is closed");
      spanError(this.#span, error);
      this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
      return Promise.reject(error);
    }
    const delivery = this.#tail.then(
      () =>
        new Promise<void>((resolve, reject) => {
          this.#write(value, (error) => {
            if (error === undefined || error === null) {
              this.#span?.addEvent("send");
              resolve();
            } else {
              spanError(this.#span, error);
              this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
              reject(error);
            }
          });
        })
    );
    this.#tail = delivery.catch(() => undefined);
    return delivery;
  }

  public async close(): Promise<void> {
    this.#active = false;
    await this.#tail;
  }
}

interface PendingRequest<HandlerState, T, ResR, R, E> {
  readonly state: HandlerState;
  readonly result: RequestResult<HandlerState, T, ResR, R, E>;
  readonly sender: Sender<ResR>;
}

function consumePendingResult<HandlerState, ReqT, ResR, T, R, E>(
  context: MessageContext,
  value: R,
  pendingRequests: ReadonlyMap<string, PendingRequest<HandlerState, T, ResR, R, E>>,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>,
  streamContext: StreamContext<T, R, E>,
  endpoint: InputEndpoint
): Completion {
  const streamId = context.streamId();
  if (streamId === undefined) {
    endpoint.onMissingStreamId(context);
    return;
  }
  const pending = pendingRequests.get(streamId);
  if (pending === undefined) {
    endpoint.onLateResult(context, streamId);
    return;
  }
  if (!pending.result.beginCallback()) {
    endpoint.onLateResult(context, streamId);
    pending.result.span()?.addEvent("late_result");
    return;
  }
  let asynchronous = false;
  try {
    const messageId = handler.getMessageId(context, streamContext, pending.state, value);
    const callback = pending.result.callback(messageId);
    if (callback === undefined) {
      endpoint.onUnknownMessageId(context, streamId, messageId);
      pending.result
        .span()
        ?.addEvent("unknown_message_id", [stringAttribute("message_id", messageId)]);
      return;
    }
    const finish = (remove: boolean): void => {
      if (remove && !pending.result.remove(messageId, callback)) {
        endpoint.onDuplicateMessageId(context, streamId, messageId);
        pending.result
          .span()
          ?.addEvent("duplicate_message_id", [stringAttribute("message_id", messageId)]);
      }
      pending.result
        .span()
        ?.addEvent("result_consumed", [stringAttribute("message_id", messageId)]);
    };
    const consumed = callback(context, streamContext, pending.state, value, pending.sender);
    if (consumed instanceof Promise) {
      asynchronous = true;
      return consumed.then(finish).finally(() => {
        pending.result.endCallback();
      });
    }
    finish(consumed);
  } finally {
    if (!asynchronous) pending.result.endCallback();
  }
}

function observeGrpcHandler(
  handler: Promise<void>,
  endpoint: InputEndpoint,
  failTransport: (error: Error) => void
): void {
  void handler.catch((value: unknown) => {
    const failure = errorFromUnknown(value);
    endpoint
      .runtimeEnvironment()
      .log()
      .error(
        Context.background(),
        "gRPC request handler failed",
        str("endpoint", endpoint.name),
        err(failure)
      );
    try {
      failTransport(failure);
    } catch (transportError: unknown) {
      endpoint
        .runtimeEnvironment()
        .log()
        .error(
          Context.background(),
          "gRPC request failure reporting failed",
          str("endpoint", endpoint.name),
          err(errorFromUnknown(transportError))
        );
    }
  });
}

abstract class GrpcStreamingSourceConsumer<HandlerState, ReqT, ResR, T, R, E>
  extends DataSourceEndpointConsumer<T, R, E>
  implements InputEndpointConsumer
{
  protected readonly handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  protected readonly streamContext: StreamContext<T, R, E>;
  protected readonly pending = new Map<string, PendingRequest<HandlerState, T, ResR, R, E>>();
  protected readonly tracer: Tracer | undefined;

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    super(endpoint, stream);
    this.handler = handler;
    this.streamContext = makeStreamContext(
      stream,
      stream.resultStream(),
      new FunctionCollector((context, value: T) => stream.consume(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
    if (stream.resultStream() !== undefined) {
      stream.setResultConsumer({ consume: (context, value) => this.consumeResult(context, value) });
    }
    this.tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  protected hasResult(): boolean {
    return this.stream().resultStream() !== undefined;
  }

  protected requestContext(call: GrpcServerCallContext): {
    context: MessageContext;
    span: Span | undefined;
  } {
    let context = contextFromCall(call);
    let span: Span | undefined;
    if (this.tracer !== undefined && context.samplingEnabled()) {
      const started = this.tracer.start(context, "grpc.input", [
        stringAttribute("stream", this.stream().name),
        stringAttribute("endpoint", this.endpoint().name)
      ]);
      context = started.context;
      span = started.span;
    }
    return { context, span };
  }

  protected addPending(
    context: MessageContext,
    state: HandlerState,
    result: RequestResult<HandlerState, T, ResR, R, E>,
    sender: Sender<ResR>
  ): string {
    const streamId = context.streamId() ?? newStreamId();
    if (this.pending.has(streamId)) throw new Error("duplicate key");
    this.pending.set(streamId, { state, result, sender });
    this.endpoint().onPendingAdd(context, streamId);
    return streamId;
  }

  protected removePending(context: MessageContext, streamId: string): void {
    if (this.pending.delete(streamId)) this.endpoint().onPendingRemove(context, streamId);
  }

  private consumeResult(context: MessageContext, value: R): Completion {
    return consumePendingResult(
      context,
      value,
      this.pending,
      this.handler,
      this.streamContext,
      this.endpoint()
    );
  }
}

class GrpcUnaryEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>
  extends DataSourceEndpointConsumer<T, R, E>
  implements InputEndpointConsumer
{
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #streamContext: StreamContext<T, R, E>;
  readonly #pending = new Map<string, PendingRequest<HandlerState, T, ResR, R, E>>();
  readonly #tracer: Tracer | undefined;

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    super(endpoint, stream);
    this.#handler = handler;
    this.#streamContext = makeStreamContext(
      stream,
      stream.resultStream(),
      new FunctionCollector((context, value: T) => stream.consume(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
    stream.setResultConsumer({ consume: (context, value) => this.consumeResult(context, value) });
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public handle(): handleUnaryCall<ReqT, ResR> {
    return (call, callback) => {
      let completed = false;
      const complete = (error: Error | null, value?: ResR): void => {
        if (completed) return;
        completed = true;
        callback(error, value);
      };
      observeGrpcHandler(this.handleCall(call, complete), this.endpoint(), (failure) => {
        complete(failure);
      });
    };
  }

  private async handleCall(
    call: ServerUnaryCall<ReqT, ResR>,
    callback: (error: Error | null, value?: ResR) => void
  ): Promise<void> {
    let context = contextFromCall(call);
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "grpc.input", [
        stringAttribute("stream", this.stream().name),
        stringAttribute("endpoint", this.endpoint().name)
      ]);
      context = started.context;
      span = started.span;
    }
    let state: HandlerState;
    try {
      const starting = this.#handler.beginRequest(context, this.#streamContext);
      const started = starting instanceof Promise ? await starting : starting;
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      callback(failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(context);
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    const hasResult = this.stream().resultStream() !== undefined;
    span?.setAttributes([
      stringAttribute("stream_id", streamId),
      boolAttribute("has_result", hasResult)
    ]);
    const result = new RequestResult<HandlerState, T, ResR, R, E>(span, false);
    let response: ResR | undefined;
    const sender = new UnarySender<ResR>((value) => {
      response = value;
      result.done();
    }, span);
    let failure: Error | undefined;
    let resultWaitFailed = false;
    let phase = "consume_message";
    if (hasResult) {
      if (this.#pending.has(streamId)) {
        failure = new Error("duplicate key");
        const ending = this.#handler.endRequest(context, this.#streamContext, failure, state);
        if (ending !== undefined) await ending;
        this.endpoint().onRequestEnd(context, startedAt, failure);
        callback(failure);
        span?.end();
        return;
      }
      this.#pending.set(streamId, { state, result, sender });
      this.endpoint().onPendingAdd(context, streamId);
    }
    try {
      const consuming = this.#handler.consumeMessage(
        context,
        this.#streamContext,
        state,
        call.request,
        result,
        sender
      );
      if (consuming !== undefined) await consuming;
      span?.addEvent("consume_message");
      phase = "eof";
      const eof = this.#handler.eof(context, this.#streamContext, state);
      if (eof !== undefined) await eof;
      span?.addEvent("eof");
      if (hasResult) {
        phase = "result";
        const waiting = waitForResult(result, context);
        const waitFailure = waiting instanceof Promise ? await waiting : waiting;
        if (waitFailure !== undefined) {
          resultWaitFailed = true;
          throw waitFailure;
        }
        span?.addEvent("result_received");
      }
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (phase === "consume_message") {
        span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
      } else if (phase === "result") {
        span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
      }
    } finally {
      if (hasResult) {
        const resultCompleted = await result.retire();
        if (resultWaitFailed && resultCompleted) failure = undefined;
        this.#pending.delete(streamId);
        this.endpoint().onPendingRemove(context, streamId);
      }
      if (failure !== undefined) spanError(span, failure);
      try {
        const ending = this.#handler.endRequest(context, this.#streamContext, failure, state);
        if (ending !== undefined) await ending;
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        spanError(span, failure);
      }
      try {
        this.endpoint().onRequestEnd(context, startedAt, failure);
      } finally {
        span?.end();
      }
    }
    if (failure !== undefined) callback(failure);
    else if (response === undefined) callback(new Error("unary gRPC handler produced no response"));
    else callback(null, response);
  }

  private consumeResult(context: MessageContext, value: R): Completion {
    return consumePendingResult(
      context,
      value,
      this.#pending,
      this.#handler,
      this.#streamContext,
      this.endpoint()
    );
  }
}

class GrpcClientStreamingEndpointConsumer<
  HandlerState,
  ReqT,
  ResR,
  T,
  R,
  E
> extends GrpcStreamingSourceConsumer<HandlerState, ReqT, ResR, T, R, E> {
  readonly #method: DescMethod;

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    method: DescMethod,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    super(endpoint, stream, handler);
    this.#method = method;
  }

  public handle(): handleClientStreamingCall<ReqT, ResR> {
    return (call, callback) => {
      let completed = false;
      const complete = (error: Error | null, value?: ResR | null): void => {
        if (completed) return;
        completed = true;
        callback(error, value);
      };
      observeGrpcHandler(this.handleCall(call, complete), this.endpoint(), (failure) => {
        complete(failure);
      });
    };
  }

  private async handleCall(
    call: ServerReadableStream<ReqT, ResR>,
    callback: (error: Error | null, value?: ResR | null) => void
  ): Promise<void> {
    const request = this.requestContext(call);
    let { context } = request;
    const { span } = request;
    let state: HandlerState;
    try {
      const started = await this.handler.beginRequest(context, this.streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      callback(failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(context);
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    const hasResult = this.hasResult();
    span?.setAttributes([
      stringAttribute("stream_id", streamId),
      boolAttribute("has_result", hasResult)
    ]);
    const result = new RequestResult<HandlerState, T, ResR, R, E>(span, hasResult);
    let response: ResR | undefined;
    const sender = new UnarySender<ResR>(
      (value) => {
        response = value;
        result.done();
      },
      span,
      false
    );
    let pending = false;
    let failure: Error | undefined;
    let resultWaitFailed = false;
    let phase = "recv";
    try {
      if (hasResult) {
        this.addPending(context, state, result, sender);
        pending = true;
      }
      const requests: AsyncIterable<ReqT> = call;
      let messageCount = 0;
      for await (const request of requests) {
        phase = "consume_message";
        await this.handler.consumeMessage(
          context,
          this.streamContext,
          state,
          request,
          result,
          sender
        );
        messageCount += 1;
        phase = "recv";
      }
      span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
      phase = "eof";
      await this.handler.eof(context, this.streamContext, state);
      if (hasResult) {
        phase = "result";
        const waiting = waitForResult(result, context);
        const waitFailure = waiting instanceof Promise ? await waiting : waiting;
        if (waitFailure !== undefined) {
          resultWaitFailed = true;
          throw waitFailure;
        }
        span?.addEvent("done_received");
      }
      if (!hasResult) {
        sender.send(context, create(this.#method.output) as ResR);
      }
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (phase === "consume_message") {
        span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
      } else if (phase === "recv") {
        span?.addEvent("recv.error", [stringAttribute("error", failure.message)]);
      } else if (phase === "result") {
        span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
      }
    } finally {
      if (pending) {
        const resultCompleted = await result.retire();
        if (resultWaitFailed && resultCompleted) failure = undefined;
        this.removePending(context, streamId);
      }
      if (failure !== undefined) spanError(span, failure);
      try {
        await this.handler.endRequest(context, this.streamContext, failure, state);
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        spanError(span, failure);
      }
      try {
        this.endpoint().onRequestEnd(context, startedAt, failure);
      } finally {
        span?.end();
      }
    }
    if (failure !== undefined) callback(failure);
    else callback(null, response);
  }
}

class GrpcServerStreamingEndpointConsumer<
  HandlerState,
  ReqT,
  ResR,
  T,
  R,
  E
> extends GrpcStreamingSourceConsumer<HandlerState, ReqT, ResR, T, R, E> {
  public handle(): handleServerStreamingCall<ReqT, ResR> {
    return (call) => {
      observeGrpcHandler(this.handleCall(call), this.endpoint(), (failure) => {
        call.destroy(failure);
      });
    };
  }

  private async handleCall(call: ServerWritableStream<ReqT, ResR>): Promise<void> {
    const request = this.requestContext(call);
    let { context } = request;
    const { span } = request;
    const sender = new StreamingSender<ResR>(
      (value, callback) => call.write(value, callback),
      span
    );
    let state: HandlerState;
    try {
      const started = await this.handler.beginRequest(context, this.streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      await sender.close();
      call.destroy(failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(context);
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    const hasResult = this.hasResult();
    span?.setAttributes([
      stringAttribute("stream_id", streamId),
      boolAttribute("has_result", hasResult)
    ]);
    const result = new RequestResult<HandlerState, T, ResR, R, E>(span, hasResult);
    let pending = false;
    let failure: Error | undefined;
    let resultWaitFailed = false;
    let phase = "consume_message";
    try {
      if (hasResult) {
        this.addPending(context, state, result, sender);
        pending = true;
      }
      await this.handler.consumeMessage(
        context,
        this.streamContext,
        state,
        call.request,
        result,
        sender
      );
      span?.addEvent("consume_message");
      phase = "eof";
      await this.handler.eof(context, this.streamContext, state);
      span?.addEvent("eof");
      if (hasResult) {
        phase = "result";
        const waiting = waitForResult(result, context);
        const waitFailure = waiting instanceof Promise ? await waiting : waiting;
        if (waitFailure !== undefined) {
          resultWaitFailed = true;
          throw waitFailure;
        }
        span?.addEvent("done_received");
      }
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (phase === "consume_message") {
        span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
      } else if (phase === "result") {
        span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
      }
    } finally {
      if (pending) {
        const resultCompleted = await result.retire();
        if (resultWaitFailed && resultCompleted) failure = undefined;
        this.removePending(context, streamId);
      }
      if (failure !== undefined) spanError(span, failure);
      try {
        await this.handler.endRequest(context, this.streamContext, failure, state);
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        spanError(span, failure);
      }
      await sender.close();
      try {
        this.endpoint().onRequestEnd(context, startedAt, failure);
      } finally {
        span?.end();
      }
    }
    if (failure === undefined) call.end();
    else call.destroy(failure);
  }
}

class GrpcBidiStreamingEndpointConsumer<
  HandlerState,
  ReqT,
  ResR,
  T,
  R,
  E
> extends GrpcStreamingSourceConsumer<HandlerState, ReqT, ResR, T, R, E> {
  public handle(): handleBidiStreamingCall<ReqT, ResR> {
    return (call) => {
      observeGrpcHandler(this.handleCall(call), this.endpoint(), (failure) => {
        call.destroy(failure);
      });
    };
  }

  private async handleCall(call: ServerDuplexStream<ReqT, ResR>): Promise<void> {
    const request = this.requestContext(call);
    let { context } = request;
    const { span } = request;
    const sender = new StreamingSender<ResR>(
      (value, callback) => call.write(value, callback),
      span
    );
    let state: HandlerState;
    try {
      const started = await this.handler.beginRequest(context, this.streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      await sender.close();
      call.destroy(failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(context);
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    const hasResult = this.hasResult();
    span?.setAttributes([
      stringAttribute("stream_id", streamId),
      boolAttribute("has_result", hasResult)
    ]);
    const result = new RequestResult<HandlerState, T, ResR, R, E>(span, hasResult);
    let pending = false;
    let failure: Error | undefined;
    let resultWaitFailed = false;
    let phase = "recv";
    try {
      if (hasResult) {
        this.addPending(context, state, result, sender);
        pending = true;
      }
      // Reading the request half must not destroy the duplex response half when
      // the client half-closes it. The response stream remains active through
      // eof/result delivery, exactly like the canonical bidirectional endpoint.
      const requests: AsyncIterable<ReqT> = call.iterator({ destroyOnReturn: false });
      let messageCount = 0;
      for await (const request of requests) {
        phase = "consume_message";
        await this.handler.consumeMessage(
          context,
          this.streamContext,
          state,
          request,
          result,
          sender
        );
        messageCount += 1;
        phase = "recv";
      }
      span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
      phase = "eof";
      await this.handler.eof(context, this.streamContext, state);
      if (hasResult) {
        phase = "result";
        const waiting = waitForResult(result, context);
        const waitFailure = waiting instanceof Promise ? await waiting : waiting;
        if (waitFailure !== undefined) {
          resultWaitFailed = true;
          throw waitFailure;
        }
        span?.addEvent("done_received");
      }
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (phase === "consume_message") {
        span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
      } else if (phase === "recv") {
        span?.addEvent("recv.error", [stringAttribute("error", failure.message)]);
      } else if (phase === "result") {
        span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
      }
    } finally {
      if (pending) {
        const resultCompleted = await result.retire();
        if (resultWaitFailed && resultCompleted) failure = undefined;
        this.removePending(context, streamId);
      }
      if (failure !== undefined) spanError(span, failure);
      try {
        await this.handler.endRequest(context, this.streamContext, failure, state);
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        spanError(span, failure);
      }
      await sender.close();
      try {
        this.endpoint().onRequestEnd(context, startedAt, failure);
      } finally {
        span?.end();
      }
    }
    if (failure === undefined) call.end();
    else call.destroy(failure);
  }
}

export function makeGrpcNoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "unary") throw new Error(`gRPC method ${method.name} is not unary`);
  const environment = stream.runtimeEnvironment();
  const endpointConfig = requireGrpcEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
  if (dataSource.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new DataSourceEndpoint(dataSource, endpointConfig.id);
  const consumer = new GrpcUnaryEndpointConsumer(endpoint, stream, handler);
  endpoint.addEndpointConsumer(consumer);
  dataSource.addEndpoint(endpoint);
  dataSource.add(service, method, consumer.handle());
  return consumer;
}

export function makeGrpcClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "client_streaming")
    throw new Error(`gRPC method ${method.name} is not client-streaming`);
  const [dataSource, endpoint] = createSourceEndpoint(stream);
  const consumer = new GrpcClientStreamingEndpointConsumer(endpoint, stream, method, handler);
  bindSourceEndpoint(dataSource, endpoint, service, method, consumer, consumer.handle());
  return consumer;
}

export function makeGrpcServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "server_streaming")
    throw new Error(`gRPC method ${method.name} is not server-streaming`);
  const [dataSource, endpoint] = createSourceEndpoint(stream);
  const consumer = new GrpcServerStreamingEndpointConsumer(endpoint, stream, handler);
  bindSourceEndpoint(dataSource, endpoint, service, method, consumer, consumer.handle());
  return consumer;
}

export function makeGrpcBidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "bidi_streaming")
    throw new Error(`gRPC method ${method.name} is not bidirectional-streaming`);
  const [dataSource, endpoint] = createSourceEndpoint(stream);
  const consumer = new GrpcBidiStreamingEndpointConsumer(endpoint, stream, handler);
  bindSourceEndpoint(dataSource, endpoint, service, method, consumer, consumer.handle());
  return consumer;
}

function createSourceEndpoint<T, R, E>(
  stream: TypedInputStream<T, R, E>
): readonly [GrpcJsDataSource, DataSourceEndpoint] {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = requireGrpcEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
  if (dataSource.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new DataSourceEndpoint(dataSource, endpointConfig.id);
  return [dataSource, endpoint];
}

function bindSourceEndpoint<T, R, E>(
  dataSource: GrpcJsDataSource,
  endpoint: DataSourceEndpoint,
  service: DescService,
  method: DescMethod,
  consumer: DataSourceEndpointConsumer<T, R, E>,
  handler: UntypedHandleCall
): void {
  endpoint.addEndpointConsumer(consumer);
  dataSource.addEndpoint(endpoint);
  dataSource.add(service, method, handler);
}

function getOrCreateDataSource(
  connectorId: number,
  environment: RuntimeEnvironment
): GrpcJsDataSource {
  const existing = environment.dataSourceById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof GrpcJsDataSource))
      throw new Error(`data source ${String(connectorId)} is not gRPC`);
    return existing;
  }
  requireGrpcDataConnectorConfig(environment.runtimeConfig().dataConnectorById(connectorId));
  const source = new GrpcJsDataSource(connectorId, environment);
  environment.addDataSource(source);
  return source;
}

function contextFromMetadata(metadata: Metadata): MessageContext {
  const values = new Map<string, string>();
  for (const key of [
    STREAM_ID_HEADER,
    TRACE_SAMPLING_HEADER,
    "traceparent",
    "tracestate",
    "baggage"
  ]) {
    const value = metadata.get(key)[0];
    if (value !== undefined) {
      values.set(key, typeof value === "string" ? value : value.toString("utf8"));
    }
  }
  if (!values.has(STREAM_ID_HEADER)) values.set(STREAM_ID_HEADER, newStreamId());
  return new MessageContext().withMetadata(values);
}

function contextFromCall(call: GrpcServerCallContext): MessageContext {
  const controller = new AbortController();
  call.once("cancelled", () => {
    controller.abort(new Error("gRPC call cancelled"));
  });
  let context = contextFromMetadata(call.metadata).withExternalCancellation(controller.signal);
  const deadline = call.getDeadline();
  const deadlineTimestamp = deadline instanceof Date ? deadline.getTime() : deadline;
  if (Number.isFinite(deadlineTimestamp)) {
    context = context.bounded(Math.max(0, deadlineTimestamp - Date.now()));
  }
  return context;
}

function waitForResult<HandlerState, T, ResR, R, E>(
  result: RequestResult<HandlerState, T, ResR, R, E>,
  context: MessageContext
): Error | undefined | Promise<Error | undefined> {
  if (context.cancelled())
    return errorFromUnknown(context.signal().reason ?? new Error("gRPC call cancelled"));
  if (result.completed()) return undefined;
  return waitForResultCompletion(result, context);
}

async function waitForResultCompletion<HandlerState, T, ResR, R, E>(
  result: RequestResult<HandlerState, T, ResR, R, E>,
  context: MessageContext
): Promise<Error | undefined> {
  let cancelled: (() => void) | undefined;
  try {
    return await Promise.race([
      result.wait().then(() => undefined),
      new Promise<Error>((resolve) => {
        cancelled = () => {
          resolve(errorFromUnknown(context.signal().reason ?? new Error("gRPC call cancelled")));
        };
        context.signal().addEventListener("abort", cancelled, { once: true });
      })
    ]);
  } finally {
    if (cancelled !== undefined) context.signal().removeEventListener("abort", cancelled);
  }
}

function serviceDefinition(service: DescService): ServiceDefinition {
  return Object.fromEntries(
    service.methods.map((method) => [
      method.localName,
      {
        path: `/${service.typeName}/${method.name}`,
        requestStream:
          method.methodKind === "client_streaming" || method.methodKind === "bidi_streaming",
        responseStream:
          method.methodKind === "server_streaming" || method.methodKind === "bidi_streaming",
        requestSerialize: (value: unknown) => Buffer.from(serialize(method.input, value)),
        requestDeserialize: (bytes: Buffer) => deserialize(method.input, bytes),
        responseSerialize: (value: unknown) => Buffer.from(serialize(method.output, value)),
        responseDeserialize: (bytes: Buffer) => deserialize(method.output, bytes)
      }
    ])
  );
}

function serialize(schema: DescMessage, value: unknown): Uint8Array {
  return toBinary(schema, value as MessageShape<DescMessage>);
}

function deserialize(schema: DescMessage, bytes: Uint8Array): unknown {
  return fromBinary(schema, bytes);
}
