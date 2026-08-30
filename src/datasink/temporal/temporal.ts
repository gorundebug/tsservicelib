import {
  DataConnectorType,
  DataSinkEndpoint,
  OutputDataSink,
  errorFromUnknown,
  newStreamId,
  spanError,
  stringAttribute,
  type Consumer,
  type Context,
  type OutputEndpointConsumer,
  type MessageContext,
  type RuntimeEnvironment,
  type SinkEndpoint,
  type Span,
  type Stream,
  type Tracer,
  type TypedSinkStream,
  type TypedSinkStreamWithResult
} from "../../runtime/index.js";
import {
  makeTemporalConnector,
  type TemporalConnector
} from "../../datasource/temporal/connector.js";
import type { EndpointEnvelope } from "../../datasource/temporal/contracts.js";

class TemporalDataSink extends OutputDataSink {
  readonly #active = new Set<Promise<void>>();

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    if (this.config().type !== DataConnectorType.Temporal) {
      throw new Error(`data sink ${this.name} is not Temporal`);
    }
  }

  public start(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public stop(_context: Context): Promise<void> {
    void _context;
    return this.#drain();
  }

  public track(operation: Promise<void>): Promise<void> {
    this.#active.add(operation);
    const remove = (): void => {
      this.#active.delete(operation);
    };
    void operation.then(remove, remove);
    return operation;
  }

  async #drain(): Promise<void> {
    while (this.#active.size > 0) {
      await Promise.allSettled([...this.#active]);
    }
  }
}

export interface TemporalEndpointHandler<State, T> {
  beginRequest(context: MessageContext, stream: Stream): Promise<State>;
  getMessageId(context: MessageContext, stream: Stream, state: State, value: T): string;
  endRequest(
    context: MessageContext,
    stream: Stream,
    error: Error | undefined,
    state: State
  ): Promise<void>;
}

class DirectTemporalEndpointHandler<T> implements TemporalEndpointHandler<
  Readonly<Record<string, never>>,
  T
> {
  public beginRequest(
    context: MessageContext,
    stream: Stream
  ): Promise<Readonly<Record<string, never>>> {
    void context;
    void stream;
    return Promise.resolve({});
  }

  public getMessageId(
    context: MessageContext,
    stream: Stream,
    state: Readonly<Record<string, never>>,
    value: T
  ): string {
    void stream;
    void state;
    void value;
    return context.streamId() ?? "";
  }

  public endRequest(
    context: MessageContext,
    stream: Stream,
    error: Error | undefined,
    state: Readonly<Record<string, never>>
  ): Promise<void> {
    void context;
    void stream;
    void error;
    void state;
    return Promise.resolve();
  }
}

class TemporalSinkConsumer<State, T, R, E> implements Consumer<T>, OutputEndpointConsumer {
  readonly #tracer: Tracer | undefined;

  public constructor(
    private readonly sinkEndpoint: DataSinkEndpoint,
    private readonly dataSink: TemporalDataSink,
    private readonly connector: TemporalConnector,
    private readonly stream: TypedSinkStream<T, E> | TypedSinkStreamWithResult<T, R, E>,
    private readonly handler: TemporalEndpointHandler<State, T>,
    private readonly withResult: boolean
  ) {
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public endpoint(): SinkEndpoint {
    return this.sinkEndpoint;
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    await this.dataSink.track(this.consumeTracked(context, value));
  }

  private async consumeTracked(context: MessageContext, value: T): Promise<void> {
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const startedSpan = this.#tracer.start(context, "temporal.output", [
        stringAttribute("stream", this.stream.name),
        stringAttribute("endpoint", this.sinkEndpoint.name)
      ]);
      context = startedSpan.context;
      span = startedSpan.span;
    }
    const started = this.sinkEndpoint.onRequestStart(context);
    let failure: Error | undefined;
    let state!: State;
    let began = false;
    try {
      state = await this.handler.beginRequest(context, this.stream);
      began = true;
      const messageId =
        this.handler.getMessageId(context, this.stream, state, value) || newStreamId();
      const remainingMs = context.remainingMs();
      const envelope: EndpointEnvelope = {
        version: 1,
        endpointId: this.sinkEndpoint.id,
        messageId,
        streamId: context.streamId() ?? messageId,
        priority: context.priority() ?? 0,
        deadlineUnixMillis:
          remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
        scheduled: false,
        scheduleId: "",
        scheduledAtUnixMillis: 0,
        firedAtUnixMillis: 0,
        payload: this.stream.inputSerde().serialize(value)
      };
      const result = await this.connector.submitEndpoint(
        context,
        this.sinkEndpoint.id,
        envelope,
        this.withResult
      );
      if (this.withResult) {
        const resultStream = this.stream as TypedSinkStreamWithResult<T, R, E>;
        await resultStream.consumeResult(context, resultStream.serde().deserialize(result.payload));
      }
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      spanError(span, failure);
      throw failure;
    } finally {
      if (began) await this.handler.endRequest(context, this.stream, failure, state);
      this.sinkEndpoint.onRequestEnd(context, started, failure);
      span?.end();
    }
  }
}

export function makeTemporalSinkEndpointConsumer<T, E>(stream: TypedSinkStream<T, E>): Consumer<T> {
  return makeTemporalSinkEndpointConsumerWithHandler(
    stream,
    new DirectTemporalEndpointHandler<T>()
  );
}

export function makeTemporalSinkEndpointConsumerWithHandler<State, T, E>(
  stream: TypedSinkStream<T, E>,
  handler: TemporalEndpointHandler<State, T>
): Consumer<T> {
  return makeSinkConsumer<State, T, never, E>(stream, handler, false);
}

export function makeTemporalSinkEndpointConsumerWithResult<T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>
): Consumer<T> {
  return makeTemporalSinkEndpointConsumerWithResultHandler(
    stream,
    new DirectTemporalEndpointHandler<T>()
  );
}

export function makeTemporalSinkEndpointConsumerWithResultHandler<State, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  handler: TemporalEndpointHandler<State, T>
): Consumer<T> {
  return makeSinkConsumer(stream, handler, true);
}

function makeSinkConsumer<State, T, R, E>(
  stream: TypedSinkStream<T, E> | TypedSinkStreamWithResult<T, R, E>,
  handler: TemporalEndpointHandler<State, T>,
  withResult: boolean
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
  if (endpointConfig === undefined) {
    throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
  }
  const connector = makeTemporalConnector(endpointConfig.idDataConnector, environment);
  const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment);
  if (dataSink.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`Temporal endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new DataSinkEndpoint(dataSink, endpointConfig.id);
  const consumer = new TemporalSinkConsumer(
    endpoint,
    dataSink,
    connector,
    stream,
    handler,
    withResult
  );
  connector.registerEndpointSubmission(endpointConfig.id);
  endpoint.addEndpointConsumer(consumer);
  dataSink.addEndpoint(endpoint);
  stream.setSinkConsumer(consumer);
  return consumer;
}

function getOrCreateDataSink(
  connectorId: number,
  environment: RuntimeEnvironment
): TemporalDataSink {
  const existing = environment.dataSinkById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof TemporalDataSink)) {
      throw new Error(`data sink ${String(connectorId)} is not Temporal`);
    }
    return existing;
  }
  const dataSink = new TemporalDataSink(connectorId, environment);
  environment.addDataSink(dataSink);
  return dataSink;
}
