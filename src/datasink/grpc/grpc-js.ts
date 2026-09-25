import { makeEndpointTraceAttributes } from "../../runtime/endpoint-tracing.js";
import {
  Client,
  credentials,
  Metadata,
  type ClientDuplexStream,
  type ClientReadableStream,
  type ClientUnaryCall,
  type ClientWritableStream
} from "@grpc/grpc-js";
import {
  fromBinary,
  toBinary,
  type DescMessage,
  type DescMethod,
  type DescService,
  type MessageShape
} from "@bufbuild/protobuf";

import {
  DataSinkEndpoint,
  DataSinkEndpointConsumerWithResult,
  FunctionCollector,
  OutputDataSink,
  SinkStreamContext,
  err,
  errorFromUnknown,
  int64Attribute,
  newStreamId,
  requireGrpcDataConnectorConfig,
  requireGrpcEndpointConfig,
  settleWithinDeadline,
  spanError,
  stringAttribute,
  type Completion,
  type Consumer,
  type Context,
  type MessageContext,
  type RuntimeEnvironment,
  type SinkEndpoint,
  type Span,
  type Tracer,
  type TypedSinkStreamWithResult
} from "../../runtime/index.js";

export interface Sender<ReqT> {
  send(context: MessageContext, request: ReqT): Completion;
}

export interface ResultContext {
  done(): void;
}

export interface EndpointHandler<HandlerState, ReqT, ResR, T, R, E> {
  beginRequest(
    context: MessageContext,
    stream: SinkStreamContext<T, R, E>
  ):
    | { readonly context: MessageContext; readonly state: HandlerState }
    | Promise<{ readonly context: MessageContext; readonly state: HandlerState }>;
  consumeMessage(
    context: MessageContext,
    stream: SinkStreamContext<T, R, E>,
    state: HandlerState,
    value: Readonly<T>,
    sender: Sender<ReqT>,
    result: ResultContext
  ): Completion;
  handleResponse(
    context: MessageContext,
    stream: SinkStreamContext<T, R, E>,
    state: HandlerState,
    response: Readonly<ResR>
  ): Completion;
  endRequest(
    context: MessageContext,
    stream: SinkStreamContext<T, R, E>,
    error: Error | undefined,
    state: HandlerState
  ): Completion;
}

class RequestSender<ReqT> implements Sender<ReqT> {
  public request: ReqT | undefined;
  public send(_context: MessageContext, request: ReqT): void {
    this.request = request;
  }
}

class StreamSender<ReqT> implements Sender<ReqT> {
  readonly #write: (request: ReqT, callback: (error?: Error | null) => void) => boolean;
  #tail = Promise.resolve();
  #active = true;
  readonly #span: Span | undefined;

  public constructor(
    write: (request: ReqT, callback: (error?: Error | null) => void) => boolean,
    span: Span | undefined
  ) {
    this.#write = write;
    this.#span = span;
  }

  public send(_context: MessageContext, request: ReqT): Promise<void> {
    if (!this.#active) {
      const error = new Error("gRPC request stream is closed");
      if (this.#span !== undefined) spanError(this.#span, error);
      this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
      return Promise.reject(error);
    }
    const delivery = this.#tail.then(
      () =>
        new Promise<void>((resolve, reject) => {
          this.#write(request, (error) => {
            if (error === undefined || error === null) {
              this.#span?.addEvent("send");
              resolve();
            } else {
              if (this.#span !== undefined) spanError(this.#span, error);
              this.#span?.addEvent("send.error", [stringAttribute("error", error.message)]);
              reject(error);
            }
          });
        })
    );
    this.#tail = delivery.catch(() => undefined);
    return delivery;
  }

  public async close(close: () => void): Promise<void> {
    this.#active = false;
    await this.#tail;
    close();
  }
}

class StreamingResultContext implements ResultContext {
  readonly #done: Promise<void>;
  #resolve: (() => void) | undefined;
  readonly #span: Span | undefined;

  public constructor(span: Span | undefined) {
    this.#span = span;
    this.#done = new Promise((resolve) => {
      this.#resolve = resolve;
    });
  }

  public done(): void {
    if (this.#resolve === undefined) return;
    this.#span?.addEvent("done_called");
    this.finish();
  }

  public isDone(): boolean {
    return this.#resolve === undefined;
  }

  public finish(): void {
    if (this.#resolve === undefined) return;
    this.#resolve();
    this.#resolve = undefined;
  }

  public wait(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.reject(errorFromUnknown(signal.reason));
    let rejectCancellation: ((error: Error) => void) | undefined;
    const cancellation = new Promise<never>((_resolve, reject) => {
      rejectCancellation = reject;
    });
    const onAbort = (): void => {
      rejectCancellation?.(errorFromUnknown(signal.reason));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    return Promise.race([this.#done, cancellation]).finally(() => {
      signal.removeEventListener("abort", onAbort);
    });
  }
}

const unaryResultContext: ResultContext = { done: () => undefined };

export class GrpcJsDataSink extends OutputDataSink {
  readonly #service: DescService;
  readonly #clients: readonly Client[];
  readonly #tracingEnabled: boolean;
  #nextClient = 0;
  readonly #codecs = new WeakMap<
    DescMethod,
    {
      path: string;
      encode: (value: unknown) => Buffer;
      decode: (bytes: Buffer) => unknown;
    }
  >();
  #started = false;
  readonly #tasks = new Set<Promise<void>>();

  public constructor(connectorId: number, environment: RuntimeEnvironment, service: DescService) {
    super(connectorId, environment);
    const config = requireGrpcDataConnectorConfig(this.config());
    if (config.address === undefined || config.address.length === 0)
      throw new Error(`gRPC data connector ${config.name} has no address`);
    const address = config.address;
    this.#service = service;
    this.#tracingEnabled = environment.tracing() !== undefined;
    this.#clients = Array.from(
      { length: config.connectionsCount },
      () =>
        new Client(address, credentials.createInsecure(), {
          "grpc.use_local_subchannel_pool": 1
        })
    );
  }

  public service(): DescService {
    return this.#service;
  }

  public start(context: Context): Promise<void> {
    void context;
    if (this.#started)
      return Promise.reject(new Error(`gRPC data sink ${this.name} already started`));
    this.#started = true;
    return Promise.resolve();
  }

  public async stop(context: Context): Promise<void> {
    if (this.#started) {
      this.#started = false;
      const drain = Promise.allSettled([...this.#tasks]).then(() => undefined);
      try {
        const [finished] = await settleWithinDeadline(context, [drain]);
        if (finished === undefined) {
          this.runtimeEnvironment().log().warn(context, "gRPC data sink shutdown timed out");
        }
      } finally {
        for (const client of this.#clients) client.close();
      }
    }
  }

  public track(context: Context, task: Promise<void>): void {
    const observed = task
      .catch((error: unknown) => {
        this.runtimeEnvironment()
          .log()
          .error(context, "gRPC background task failed", err(errorFromUnknown(error)));
      })
      .finally(() => {
        this.#tasks.delete(observed);
      });
    this.#tasks.add(observed);
  }

  public unary<ResR>(context: MessageContext, method: DescMethod, request: unknown): Promise<ResR> {
    const metadata = metadataFromContext(context, this.#tracingEnabled);
    const remainingMs = context.remainingMs();
    return new Promise((resolve, reject) => {
      const codec = this.codec(method);
      const call: ClientUnaryCall = this.nextClient().makeUnaryRequest(
        codec.path,
        codec.encode,
        codec.decode as (bytes: Buffer) => ResR,
        request,
        metadata,
        remainingMs === undefined ? {} : { deadline: Date.now() + remainingMs },
        (error, response) => {
          context.signal().removeEventListener("abort", cancel);
          if (error !== null) reject(error);
          else if (response === undefined)
            reject(new Error("unary gRPC call returned no response"));
          else resolve(response);
        }
      );
      const cancel = (): void => {
        call.cancel();
      };
      if (context.cancelled()) cancel();
      else context.signal().addEventListener("abort", cancel, { once: true });
    });
  }

  public serverStream<ResR>(
    context: MessageContext,
    method: DescMethod,
    request: unknown
  ): ClientReadableStream<ResR> {
    const codec = this.codec(method);
    const call = this.nextClient().makeServerStreamRequest(
      codec.path,
      codec.encode,
      codec.decode as (bytes: Buffer) => ResR,
      request,
      metadataFromContext(context, this.#tracingEnabled),
      callOptions(context)
    );
    bindCancellation(context, call);
    return call;
  }

  public clientStream<ReqT, ResR>(
    context: MessageContext,
    method: DescMethod
  ): readonly [ClientWritableStream<ReqT>, Promise<ResR>] {
    let call: ClientWritableStream<ReqT> | undefined;
    const response = new Promise<ResR>((resolve, reject) => {
      const codec = this.codec(method);
      call = this.nextClient().makeClientStreamRequest(
        codec.path,
        codec.encode,
        codec.decode as (bytes: Buffer) => ResR,
        metadataFromContext(context, this.#tracingEnabled),
        callOptions(context),
        (error, value) => {
          if (error !== null) reject(error);
          else if (value === undefined)
            reject(new Error("client-streaming gRPC call returned no response"));
          else resolve(value);
        }
      );
    });
    if (call === undefined) throw new Error("gRPC client stream was not created");
    bindCancellation(context, call);
    return [call, response];
  }

  public bidiStream<ReqT, ResR>(
    context: MessageContext,
    method: DescMethod
  ): ClientDuplexStream<ReqT, ResR> {
    const codec = this.codec(method);
    const call = this.nextClient().makeBidiStreamRequest(
      codec.path,
      codec.encode,
      codec.decode as (bytes: Buffer) => ResR,
      metadataFromContext(context, this.#tracingEnabled),
      callOptions(context)
    );
    bindCancellation(context, call);
    return call;
  }

  private codec(method: DescMethod) {
    let codec = this.#codecs.get(method);
    if (codec === undefined) {
      codec = {
        path: `/${this.#service.typeName}/${method.name}`,
        encode: (value: unknown) => serialize(method.input, value),
        decode: (bytes: Buffer) => deserialize(method.output, bytes)
      };
      this.#codecs.set(method, codec);
    }
    return codec;
  }

  private nextClient(): Client {
    const client = this.#clients[this.#nextClient];
    if (client === undefined) throw new Error(`gRPC data sink ${this.name} has no clients`);
    this.#nextClient = (this.#nextClient + 1) % this.#clients.length;
    return client;
  }
}

class GrpcUnaryEndpointConsumer<HandlerState, ReqT, ResR, T, R, E> implements Consumer<T> {
  readonly #base: DataSinkEndpointConsumerWithResult<T, R, E>;
  readonly #streamContext: SinkStreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #method: DescMethod;
  readonly #traceAttributes: ReturnType<typeof makeEndpointTraceAttributes>;
  readonly #tracer: Tracer | undefined;

  public constructor(
    endpoint: DataSinkEndpoint,
    stream: TypedSinkStreamWithResult<T, R, E>,
    method: DescMethod,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
    this.#streamContext = new SinkStreamContext(
      stream,
      stream.runtimeEnvironment().log(),
      new FunctionCollector((context, value: R) => stream.consumeResult(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
    this.#method = method;
    this.#handler = handler;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    this.#traceAttributes = makeEndpointTraceAttributes(stream, endpoint.name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "grpc.output", this.#traceAttributes);
      context = started.context;
      span = started.span;
    }
    let state: HandlerState;
    let handlerContext: MessageContext;
    try {
      const starting = this.#handler.beginRequest(context, this.#streamContext);
      const started = starting instanceof Promise ? await starting : starting;
      state = started.state;
      handlerContext = started.context;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(handlerContext);
    let failure: Error | undefined;
    let phase = "consume_message";
    try {
      const sender = new RequestSender<ReqT>();
      const consuming = this.#handler.consumeMessage(
        handlerContext,
        this.#streamContext,
        state,
        value,
        sender,
        unaryResultContext
      );
      if (consuming !== undefined) await consuming;
      span?.addEvent("consume_message");
      if (sender.request === undefined) throw new Error("gRPC sink handler produced no request");
      const dataSink = this.endpoint().dataSink();
      if (!(dataSink instanceof GrpcJsDataSink)) throw new Error("invalid gRPC data sink");
      phase = "grpc_call";
      const requestContext = handlerContext.withStreamId(newStreamId());
      const response = await dataSink.unary<ResR>(requestContext, this.#method, sender.request);
      span?.addEvent("grpc_call");
      phase = "handle_response";
      const handling = this.#handler.handleResponse(
        handlerContext,
        this.#streamContext,
        state,
        response
      );
      if (handling !== undefined) await handling;
      span?.addEvent("handle_response");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
    } finally {
      try {
        const ending = this.#handler.endRequest(
          handlerContext,
          this.#streamContext,
          failure,
          state
        );
        if (ending !== undefined) await ending;
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        if (span !== undefined) spanError(span, failure);
      } finally {
        try {
          this.endpoint().onRequestEnd(handlerContext, startedAt, failure);
        } finally {
          span?.end();
        }
      }
    }
  }
}

class GrpcServerStreamingEndpointConsumer<
  HandlerState,
  ReqT,
  ResR,
  T,
  R,
  E
> implements Consumer<T> {
  readonly #base: DataSinkEndpointConsumerWithResult<T, R, E>;
  readonly #streamContext: SinkStreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #method: DescMethod;
  readonly #traceAttributes: ReturnType<typeof makeEndpointTraceAttributes>;
  readonly #tracer: Tracer | undefined;

  public constructor(
    endpoint: DataSinkEndpoint,
    stream: TypedSinkStreamWithResult<T, R, E>,
    method: DescMethod,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
    this.#streamContext = makeSinkContext(stream);
    this.#method = method;
    this.#handler = handler;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    this.#traceAttributes = makeEndpointTraceAttributes(stream, endpoint.name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    const traced =
      this.#tracer !== undefined && context.samplingEnabled()
        ? this.#tracer.start(context, "grpc.output", this.#traceAttributes)
        : undefined;
    context = traced?.context ?? context;
    const span = traced?.span;
    let state: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      span?.end();
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(context);
    let failure: Error | undefined;
    let phase = "consume_message";
    try {
      const sender = new RequestSender<ReqT>();
      await this.#handler.consumeMessage(
        context,
        this.#streamContext,
        state,
        value,
        sender,
        unaryResultContext
      );
      span?.addEvent("consume_message");
      if (sender.request === undefined) throw new Error("gRPC sink handler produced no request");
      const dataSink = requireGrpcJsDataSink(this.endpoint());
      phase = "grpc_call";
      const requestContext = context.withStreamId(newStreamId());
      const call = dataSink.serverStream<ResR>(requestContext, this.#method, sender.request);
      span?.addEvent("grpc_call");
      const responses: AsyncIterable<ResR> = call;
      let messageCount = 0;
      phase = "recv";
      for await (const response of responses) {
        phase = "handle_response";
        await this.#handler.handleResponse(context, this.#streamContext, state, response);
        messageCount += 1;
        phase = "recv";
      }
      span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
    } finally {
      try {
        await this.#handler.endRequest(context, this.#streamContext, failure, state);
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        if (span !== undefined) spanError(span, failure);
      } finally {
        try {
          this.endpoint().onRequestEnd(context, startedAt, failure);
        } finally {
          span?.end();
        }
      }
    }
  }
}

interface ClientStreamingSession<HandlerState, ReqT> {
  readonly context: MessageContext;
  readonly state: HandlerState;
  readonly sender: StreamSender<ReqT>;
  readonly result: StreamingResultContext;
  readonly span: Span | undefined;
  readonly consumers: Set<Promise<void>>;
}

class StreamingSessionSlot<HandlerState, ReqT> {
  public readonly ready: Promise<ClientStreamingSession<HandlerState, ReqT>>;
  #closing = false;
  #resolve: (session: ClientStreamingSession<HandlerState, ReqT>) => void = () => undefined;
  #reject: (error: unknown) => void = () => undefined;

  public constructor() {
    this.ready = new Promise((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    // A synchronous BeginRequest failure can close admission before any waiter attaches.
    void this.ready.catch(() => undefined);
  }

  public resolve(session: ClientStreamingSession<HandlerState, ReqT>): void {
    this.#resolve(session);
  }

  public reject(error: unknown): void {
    this.close();
    this.#reject(error);
  }

  public close(): void {
    this.#closing = true;
  }

  public isClosing(): boolean {
    return this.#closing;
  }
}

class GrpcClientStreamingEndpointConsumer<
  HandlerState,
  ReqT,
  ResR,
  T,
  R,
  E
> implements Consumer<T> {
  readonly #base: DataSinkEndpointConsumerWithResult<T, R, E>;
  readonly #streamContext: SinkStreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #method: DescMethod;
  readonly #traceAttributes: ReturnType<typeof makeEndpointTraceAttributes>;
  readonly #tracer: Tracer | undefined;
  readonly #pending = new Map<string, StreamingSessionSlot<HandlerState, ReqT>>();

  public constructor(
    endpoint: DataSinkEndpoint,
    stream: TypedSinkStreamWithResult<T, R, E>,
    method: DescMethod,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
    this.#streamContext = makeSinkContext(stream);
    this.#method = method;
    this.#handler = handler;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    this.#traceAttributes = makeEndpointTraceAttributes(stream, endpoint.name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    let pending = this.#pending.get(streamId);
    if (pending === undefined) {
      const opening = new StreamingSessionSlot<HandlerState, ReqT>();
      pending = opening;
      this.#pending.set(streamId, opening);
      requireGrpcJsDataSink(this.endpoint()).track(
        context,
        this.createSession(context, streamId, opening).then(
          (session) => {
            opening.resolve(session);
          },
          (error: unknown) => {
            opening.reject(error);
          }
        )
      );
    }
    if (pending.isClosing()) {
      this.endpoint().onBeginRequestFailed(
        context,
        new Error(`gRPC session ${streamId} is still completing`)
      );
      return;
    }
    let session: ClientStreamingSession<HandlerState, ReqT>;
    try {
      session = await pending.ready;
    } catch {
      return;
    }
    if (pending.isClosing() || session.result.isDone() || session.context.cancelled()) {
      this.endpoint().onBeginRequestFailed(
        context,
        new Error(`gRPC session ${streamId} is still completing`)
      );
      return;
    }
    const consume = (async () => {
      try {
        await this.#handler.consumeMessage(
          session.context,
          this.#streamContext,
          session.state,
          value,
          session.sender,
          session.result
        );
        session.span?.addEvent("consume_message");
      } catch (error: unknown) {
        const failure = errorFromUnknown(error);
        if (session.span !== undefined) spanError(session.span, failure);
        session.span?.addEvent("consume_message.error", [
          stringAttribute("error", failure.message)
        ]);
        throw failure;
      }
    })();
    session.consumers.add(consume);
    try {
      await consume;
    } catch {
      session.result.done();
    } finally {
      session.consumers.delete(consume);
    }
  }

  private async createSession(
    context: MessageContext,
    streamId: string,
    pending: StreamingSessionSlot<HandlerState, ReqT>
  ): Promise<ClientStreamingSession<HandlerState, ReqT>> {
    let state: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      pending.reject(failure);
      this.endpoint().onBeginRequestFailed(context, failure);
      this.#pending.delete(streamId);
      throw failure;
    }
    const traced =
      this.#tracer !== undefined && context.samplingEnabled()
        ? this.#tracer.start(context, "grpc.output", this.#traceAttributes)
        : undefined;
    context = traced?.context ?? context;
    const span = traced?.span;
    const startedAt = this.endpoint().onRequestStart(context);
    span?.addEvent("begin_request");
    const phase = "grpc_call";
    try {
      const dataSink = requireGrpcJsDataSink(this.endpoint());
      const requestContext = context.withStreamId(newStreamId());
      const [call, response] = dataSink.clientStream<ReqT, ResR>(requestContext, this.#method);
      span?.addEvent("grpc_call");
      const sender = new StreamSender<ReqT>(
        (request, callback) => call.write(request, callback),
        span
      );
      const result = new StreamingResultContext(span);
      const session: ClientStreamingSession<HandlerState, ReqT> = {
        context,
        state,
        sender,
        result,
        span,
        consumers: new Set()
      };
      dataSink.track(
        context,
        this.finishSession(streamId, pending, session, response, () => call.end(), startedAt, span)
      );
      return session;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
      pending.reject(failure);
      try {
        await this.#handler.endRequest(context, this.#streamContext, failure, state);
      } finally {
        try {
          this.endpoint().onRequestEnd(context, startedAt, failure);
        } finally {
          span?.end();
          this.#pending.delete(streamId);
        }
      }
      throw failure;
    }
  }

  private async finishSession(
    streamId: string,
    pending: StreamingSessionSlot<HandlerState, ReqT>,
    session: ClientStreamingSession<HandlerState, ReqT>,
    response: Promise<ResR>,
    close: () => void,
    startedAt: number | undefined,
    span: Span | undefined
  ): Promise<void> {
    let failure: Error | undefined;
    let phase = "close_and_recv";
    // Attach both outcomes immediately: a transport error can precede Done.
    const receivedResponse = response.then(
      (value) => ({ value }),
      (error: unknown) => ({ error: errorFromUnknown(error) })
    );
    try {
      await session.result.wait(session.context.signal());
      pending.close();
      await session.sender.close(close);
      const received = await receivedResponse;
      await Promise.allSettled([...session.consumers]);
      if ("error" in received) throw received.error;
      span?.addEvent("close_and_recv");
      phase = "handle_response";
      await this.#handler.handleResponse(
        session.context,
        this.#streamContext,
        session.state,
        received.value
      );
      span?.addEvent("handle_response");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
    } finally {
      pending.close();
      session.result.finish();
      await Promise.allSettled([...session.consumers]);
      try {
        await this.#handler.endRequest(
          session.context,
          this.#streamContext,
          failure,
          session.state
        );
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        if (span !== undefined) spanError(span, failure);
      } finally {
        try {
          this.endpoint().onRequestEnd(session.context, startedAt, failure);
        } finally {
          span?.end();
          this.#pending.delete(streamId);
        }
      }
    }
  }
}

class GrpcBidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E> implements Consumer<T> {
  readonly #base: DataSinkEndpointConsumerWithResult<T, R, E>;
  readonly #streamContext: SinkStreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>;
  readonly #method: DescMethod;
  readonly #traceAttributes: ReturnType<typeof makeEndpointTraceAttributes>;
  readonly #tracer: Tracer | undefined;
  readonly #pending = new Map<string, StreamingSessionSlot<HandlerState, ReqT>>();

  public constructor(
    endpoint: DataSinkEndpoint,
    stream: TypedSinkStreamWithResult<T, R, E>,
    method: DescMethod,
    handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
  ) {
    this.#base = new DataSinkEndpointConsumerWithResult(endpoint, stream);
    this.#streamContext = makeSinkContext(stream);
    this.#method = method;
    this.#handler = handler;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    this.#traceAttributes = makeEndpointTraceAttributes(stream, endpoint.name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    let pending = this.#pending.get(streamId);
    if (pending === undefined) {
      const opening = new StreamingSessionSlot<HandlerState, ReqT>();
      pending = opening;
      this.#pending.set(streamId, opening);
      requireGrpcJsDataSink(this.endpoint()).track(
        context,
        this.createSession(context, streamId, opening).then(
          (session) => {
            opening.resolve(session);
          },
          (error: unknown) => {
            opening.reject(error);
          }
        )
      );
    }
    if (pending.isClosing()) {
      this.endpoint().onBeginRequestFailed(
        context,
        new Error(`gRPC session ${streamId} is still completing`)
      );
      return;
    }
    let session: ClientStreamingSession<HandlerState, ReqT>;
    try {
      session = await pending.ready;
    } catch {
      return;
    }
    if (pending.isClosing() || session.result.isDone() || session.context.cancelled()) {
      this.endpoint().onBeginRequestFailed(
        context,
        new Error(`gRPC session ${streamId} is still completing`)
      );
      return;
    }
    const consume = (async () => {
      try {
        await this.#handler.consumeMessage(
          session.context,
          this.#streamContext,
          session.state,
          value,
          session.sender,
          session.result
        );
        session.span?.addEvent("consume_message");
      } catch (error: unknown) {
        const failure = errorFromUnknown(error);
        if (session.span !== undefined) spanError(session.span, failure);
        session.span?.addEvent("consume_message.error", [
          stringAttribute("error", failure.message)
        ]);
        throw failure;
      }
    })();
    session.consumers.add(consume);
    try {
      await consume;
    } catch {
      session.result.done();
    } finally {
      session.consumers.delete(consume);
    }
  }

  private async createSession(
    context: MessageContext,
    streamId: string,
    pending: StreamingSessionSlot<HandlerState, ReqT>
  ): Promise<ClientStreamingSession<HandlerState, ReqT>> {
    let state: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      pending.reject(failure);
      this.endpoint().onBeginRequestFailed(context, failure);
      this.#pending.delete(streamId);
      throw failure;
    }
    const traced =
      this.#tracer !== undefined && context.samplingEnabled()
        ? this.#tracer.start(context, "grpc.output", this.#traceAttributes)
        : undefined;
    context = traced?.context ?? context;
    const span = traced?.span;
    const startedAt = this.endpoint().onRequestStart(context);
    span?.addEvent("begin_request");
    const phase = "grpc_call";
    try {
      const dataSink = requireGrpcJsDataSink(this.endpoint());
      const requestContext = context.withStreamId(newStreamId());
      const call = dataSink.bidiStream<ReqT, ResR>(requestContext, this.#method);
      span?.addEvent("grpc_call");
      const sender = new StreamSender<ReqT>(
        (request, callback) => call.write(request, callback),
        span
      );
      const result = new StreamingResultContext(span);
      const session: ClientStreamingSession<HandlerState, ReqT> = {
        context,
        state,
        sender,
        result,
        span,
        consumers: new Set()
      };
      dataSink.track(
        context,
        this.finishSession(streamId, pending, session, call, startedAt, span)
      );
      return session;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      span?.addEvent(`${phase}.error`, [stringAttribute("error", failure.message)]);
      pending.reject(failure);
      try {
        await this.#handler.endRequest(context, this.#streamContext, failure, state);
      } finally {
        try {
          this.endpoint().onRequestEnd(context, startedAt, failure);
        } finally {
          span?.end();
          this.#pending.delete(streamId);
        }
      }
      throw failure;
    }
  }

  private async finishSession(
    streamId: string,
    pending: StreamingSessionSlot<HandlerState, ReqT>,
    session: ClientStreamingSession<HandlerState, ReqT>,
    call: ClientDuplexStream<ReqT, ResR>,
    startedAt: number | undefined,
    span: Span | undefined
  ): Promise<void> {
    let failure: Error | undefined;
    const receive = this.receiveResponses(session, call);
    try {
      const winner = await Promise.race([
        session.result.wait(session.context.signal()).then(() => "done" as const),
        receive.then(() => "responses" as const)
      ]);
      pending.close();
      await session.sender.close(() => call.end());
      if (winner === "done") await receive;
      span?.addEvent("done_received");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      if (span !== undefined) spanError(span, failure);
      call.cancel();
    } finally {
      pending.close();
      session.result.finish();
      await Promise.allSettled([receive, ...session.consumers]);
      try {
        await this.#handler.endRequest(
          session.context,
          this.#streamContext,
          failure,
          session.state
        );
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        if (span !== undefined) spanError(span, failure);
      } finally {
        try {
          this.endpoint().onRequestEnd(session.context, startedAt, failure);
        } finally {
          span?.end();
          this.#pending.delete(streamId);
        }
      }
    }
  }

  private async receiveResponses(
    session: ClientStreamingSession<HandlerState, ReqT>,
    call: ClientDuplexStream<ReqT, ResR>
  ): Promise<void> {
    const responses: AsyncIterable<ResR> = call;
    let messageCount = 0;
    for await (const response of responses) {
      await this.#handler.handleResponse(
        session.context,
        this.#streamContext,
        session.state,
        response
      );
      messageCount += 1;
    }
    session.span?.addEvent("eof", [int64Attribute("messages_received", BigInt(messageCount))]);
  }
}

export function makeGrpcNoStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "unary") throw new Error(`gRPC method ${method.name} is not unary`);
  const environment = stream.runtimeEnvironment();
  const endpointConfig = requireGrpcEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, service);
  const existing = dataSink.endpoint(endpointConfig.id);
  if (existing !== undefined && !(existing instanceof DataSinkEndpoint))
    throw new Error(`endpoint ${endpointConfig.name} is not a gRPC sink endpoint`);
  const endpoint = existing ?? new DataSinkEndpoint(dataSink, endpointConfig.id);
  const consumer = new GrpcUnaryEndpointConsumer(endpoint, stream, method, handler);
  endpoint.addEndpointConsumer(consumer);
  if (existing === undefined) dataSink.addEndpoint(endpoint);
  stream.setSinkConsumer(consumer);
  return consumer;
}

export function makeGrpcServerStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "server_streaming")
    throw new Error(`gRPC method ${method.name} is not server-streaming`);
  const [dataSink, endpoint] = createSinkEndpoint(stream, service);
  const consumer = new GrpcServerStreamingEndpointConsumer(endpoint, stream, method, handler);
  bindSinkEndpoint(dataSink, endpoint, stream, consumer);
  return consumer;
}

export function makeGrpcClientStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "client_streaming")
    throw new Error(`gRPC method ${method.name} is not client-streaming`);
  const [dataSink, endpoint] = createSinkEndpoint(stream, service);
  const consumer = new GrpcClientStreamingEndpointConsumer(endpoint, stream, method, handler);
  bindSinkEndpoint(dataSink, endpoint, stream, consumer);
  return consumer;
}

export function makeGrpcBidiStreamingEndpointConsumer<HandlerState, ReqT, ResR, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  service: DescService,
  method: DescMethod,
  handler: EndpointHandler<HandlerState, ReqT, ResR, T, R, E>
): Consumer<T> {
  if (method.methodKind !== "bidi_streaming")
    throw new Error(`gRPC method ${method.name} is not bidirectional-streaming`);
  const [dataSink, endpoint] = createSinkEndpoint(stream, service);
  const consumer = new GrpcBidiStreamingEndpointConsumer(endpoint, stream, method, handler);
  bindSinkEndpoint(dataSink, endpoint, stream, consumer);
  return consumer;
}

function createSinkEndpoint<T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  service: DescService
): readonly [GrpcJsDataSink, DataSinkEndpoint] {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = requireGrpcEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, service);
  const existing = dataSink.endpoint(endpointConfig.id);
  if (existing !== undefined && !(existing instanceof DataSinkEndpoint))
    throw new Error(`endpoint ${endpointConfig.name} is not a gRPC sink endpoint`);
  return [dataSink, existing ?? new DataSinkEndpoint(dataSink, endpointConfig.id)];
}

function bindSinkEndpoint<T, R, E>(
  dataSink: GrpcJsDataSink,
  endpoint: DataSinkEndpoint,
  stream: TypedSinkStreamWithResult<T, R, E>,
  consumer: Consumer<T> & { endpoint(): SinkEndpoint }
): void {
  endpoint.addEndpointConsumer(consumer);
  if (dataSink.endpoint(endpoint.id) === undefined) dataSink.addEndpoint(endpoint);
  stream.setSinkConsumer(consumer);
}

function makeSinkContext<T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>
): SinkStreamContext<T, R, E> {
  return new SinkStreamContext(
    stream,
    stream.runtimeEnvironment().log(),
    new FunctionCollector((context, value: R) => stream.consumeResult(context, value)),
    new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
  );
}

function requireGrpcJsDataSink(endpoint: SinkEndpoint): GrpcJsDataSink {
  const dataSink = endpoint.dataSink();
  if (!(dataSink instanceof GrpcJsDataSink)) throw new Error("invalid gRPC data sink");
  return dataSink;
}

function getOrCreateDataSink(
  connectorId: number,
  environment: RuntimeEnvironment,
  service: DescService
): GrpcJsDataSink {
  const existing = environment.dataSinkById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof GrpcJsDataSink))
      throw new Error(`data sink ${String(connectorId)} is not gRPC`);
    if (existing.service() !== service)
      throw new Error(`gRPC data sink ${existing.name} uses another service descriptor`);
    return existing;
  }
  const sink = new GrpcJsDataSink(connectorId, environment, service);
  environment.addDataSink(sink);
  return sink;
}

function metadataFromContext(context: MessageContext, tracingEnabled: boolean): Metadata {
  const metadata = new Metadata();
  for (const [key, value] of context.transportMetadata(tracingEnabled)) metadata.set(key, value);
  return metadata;
}

function callOptions(context: MessageContext): { readonly deadline?: number } {
  const remainingMs = context.remainingMs();
  return remainingMs === undefined ? {} : { deadline: Date.now() + remainingMs };
}

function bindCancellation(
  context: MessageContext,
  call: { cancel(): void; once(event: "status", listener: () => void): unknown }
): void {
  const cancel = (): void => {
    call.cancel();
  };
  if (context.cancelled()) cancel();
  else {
    context.signal().addEventListener("abort", cancel, { once: true });
    call.once("status", () => {
      context.signal().removeEventListener("abort", cancel);
    });
  }
}

function serialize(schema: DescMessage, value: unknown): Buffer {
  const bytes = toBinary(schema, value as MessageShape<DescMessage>);
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function deserialize(schema: DescMessage, bytes: Uint8Array): unknown {
  return fromBinary(schema, bytes);
}
