import {
  DataConnectorType,
  DataSinkEndpoint,
  DataSinkEndpointConsumer,
  FunctionCollector,
  OutputDataSink,
  Context,
  errorFromUnknown,
  spanError,
  stringAttribute,
  type Collector,
  type Completion,
  type Consumer,
  type MessageContext,
  type RuntimeEnvironment,
  type SinkEndpoint,
  type Span,
  type Stream,
  type Tracer,
  type TypedSinkStream
} from "../../runtime/index.js";

/** Canonical custom-sink lifecycle: getStreamId -> beginRequest -> consumeMessage -> endRequest. */
export interface EndpointHandler<HandlerState, T, R> {
  getStreamId(context: MessageContext, value: Readonly<T>): string;
  beginRequest(
    context: MessageContext,
    stream: Stream
  ):
    | { readonly context: MessageContext; readonly state: HandlerState }
    | Promise<{ readonly context: MessageContext; readonly state: HandlerState }>;
  consumeMessage(
    context: MessageContext,
    stream: Stream,
    handlerState: HandlerState,
    value: Readonly<T>,
    resultStream: Collector<R>
  ): Completion;
  endRequest(
    context: MessageContext,
    stream: Stream,
    error: Error | undefined,
    handlerState: HandlerState
  ): Completion;
}

export interface SinkCallback<T> {
  done(context: MessageContext, value: T, error: Error | undefined): Completion;
}

interface CustomEndpointConsumerContract<T> extends Consumer<T> {
  endpoint(): SinkEndpoint;
  start(context: Context): Promise<void>;
  stop(context: Context): Promise<void>;
}

class CustomSinkEndpoint<T> extends DataSinkEndpoint {
  #binding: CustomEndpointConsumerContract<T> | undefined;

  public bind(binding: CustomEndpointConsumerContract<T>): void {
    if (this.#binding !== undefined) {
      throw new Error(`consumer already assigned to custom endpoint ${this.name}`);
    }
    this.#binding = binding;
    this.addEndpointConsumer(binding);
  }

  public async start(context: Context): Promise<void> {
    await this.#binding?.start(context);
  }

  public async stop(context: Context): Promise<void> {
    await this.#binding?.stop(context);
  }
}

export class CustomDataSink extends OutputDataSink {
  #started = false;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    if (this.config().type !== DataConnectorType.Custom) {
      throw new Error(`data sink ${this.name} is not custom`);
    }
  }

  public async start(context: Context): Promise<void> {
    if (this.#started) throw new Error(`custom data sink ${this.name} is already started`);
    this.#started = true;
    try {
      for (const endpoint of this.customEndpoints()) await endpoint.start(context);
    } catch (error: unknown) {
      this.#started = false;
      await Promise.allSettled(
        this.customEndpoints().map(async (endpoint) => endpoint.stop(Context.background()))
      );
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) return;
    this.#started = false;
    await Promise.all(this.customEndpoints().map(async (endpoint) => endpoint.stop(context)));
  }

  private customEndpoints(): readonly CustomSinkEndpoint<unknown>[] {
    return this.endpoints().map((endpoint) => {
      if (!(endpoint instanceof CustomSinkEndpoint)) {
        throw new Error(`sink endpoint ${endpoint.name} is not custom`);
      }
      return endpoint;
    });
  }
}

class CustomEndpointConsumer<HandlerState, T, R>
  implements Consumer<T>, CustomEndpointConsumerContract<T>
{
  readonly #base: DataSinkEndpointConsumer<T, R>;
  readonly #stream: TypedSinkStream<T, R>;
  readonly #handler: EndpointHandler<HandlerState, T, R>;
  readonly #resultStream: Collector<R>;
  readonly #tracer: Tracer | undefined;
  #sinkCallback: SinkCallback<T> | undefined;

  public constructor(
    endpoint: CustomSinkEndpoint<T>,
    stream: TypedSinkStream<T, R>,
    handler: EndpointHandler<HandlerState, T, R>
  ) {
    this.#base = new DataSinkEndpointConsumer(endpoint, stream);
    this.#stream = stream;
    this.#handler = handler;
    this.#resultStream = new FunctionCollector((context, value) =>
      stream.errorStream().consume(context, value)
    );
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public setSinkCallback(callback: SinkCallback<T>): void {
    this.#sinkCallback = callback;
  }

  public start(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public stop(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    const endpoint = this.#base.endpoint();
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "local.output", [
        stringAttribute("stream", this.#stream.name),
        stringAttribute("endpoint", endpoint.name)
      ]);
      context = started.context;
      span = started.span;
    }
    try {
      await this.consumeTraced(context, value, endpoint, span);
    } finally {
      span?.end();
    }
  }

  private async consumeTraced(
    context: MessageContext,
    value: T,
    endpoint: SinkEndpoint,
    span: Span | undefined
  ): Promise<void> {
    const originalContext = context;
    const streamId = this.#handler.getStreamId(context, value);
    context = context.withStreamId(streamId);
    span?.setAttributes([stringAttribute("stream_id", streamId)]);

    let handlerContext: MessageContext;
    let handlerState: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#stream);
      handlerContext = started.context;
      handlerState = started.state;
      span?.addEvent("begin_request");
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      endpoint.onBeginRequestFailed(context, failure);
      return;
    }

    const requestStarted = endpoint.onRequestStart(handlerContext);
    let failure: Error | undefined;
    try {
      await this.#handler.consumeMessage(
        handlerContext,
        this.#stream,
        handlerState,
        value,
        this.#resultStream
      );
      span?.addEvent("consume_message");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
    } finally {
      try {
        await this.#handler.endRequest(handlerContext, this.#stream, failure, handlerState);
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        spanError(span, failure);
      } finally {
        endpoint.onRequestEnd(handlerContext, requestStarted, failure);
      }
    }
    await this.#sinkCallback?.done(originalContext, value, failure);
  }
}

export function makeCustomEndpointConsumer<HandlerState, T, R>(
  stream: TypedSinkStream<T, R>,
  handler: EndpointHandler<HandlerState, T, R>
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
  if (endpointConfig === undefined) {
    throw new Error(`endpoint config ${String(stream.endpointId())} not found`);
  }
  const connectorConfig = environment
    .runtimeConfig()
    .dataConnectorById(endpointConfig.idDataConnector);
  if (connectorConfig === undefined) {
    throw new Error(`data connector config ${String(endpointConfig.idDataConnector)} not found`);
  }
  if (connectorConfig.type !== DataConnectorType.Custom) {
    throw new Error(`data connector ${connectorConfig.name} is not custom`);
  }
  const existing = environment.dataSinkById(connectorConfig.id);
  const dataSink = existing ?? new CustomDataSink(connectorConfig.id, environment);
  if (!(dataSink instanceof CustomDataSink)) {
    throw new Error(`data sink ${connectorConfig.name} is not custom`);
  }
  if (existing === undefined) environment.addDataSink(dataSink);
  if (dataSink.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new CustomSinkEndpoint<T>(dataSink, endpointConfig.id);
  const consumer = new CustomEndpointConsumer(endpoint, stream, handler);
  endpoint.bind(consumer);
  dataSink.addEndpoint(endpoint);
  stream.setSinkConsumer(consumer);
  return consumer;
}
