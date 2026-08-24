import {
  DataConnectorType,
  DataSinkEndpoint,
  OutputDataSink,
  errorFromUnknown,
  makeTemporalConnector,
  newStreamId,
  spanError,
  stringAttribute,
  type Consumer,
  type Context,
  type EndpointEnvelope,
  type OutputEndpointConsumer,
  type MessageContext,
  type RuntimeEnvironment,
  type SinkEndpoint,
  type Span,
  type TemporalConnector,
  type Tracer,
  type TypedSinkStream,
  type TypedSinkStreamWithResult
} from "../../runtime/index.js";

class TemporalDataSink extends OutputDataSink {
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
    return Promise.resolve();
  }
}

class TemporalSinkConsumer<T, R, E> implements Consumer<T>, OutputEndpointConsumer {
  readonly #tracer: Tracer | undefined;

  public constructor(
    private readonly sinkEndpoint: DataSinkEndpoint,
    private readonly connector: TemporalConnector,
    private readonly stream: TypedSinkStream<T, E> | TypedSinkStreamWithResult<T, R, E>,
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
    try {
      const executionId = context.streamId() ?? newStreamId();
      const remainingMs = context.remainingMs();
      const envelope: EndpointEnvelope = {
        version: 1,
        endpointId: this.sinkEndpoint.id,
        executionId,
        streamId: context.streamId() ?? executionId,
        priority: context.priority() ?? 0,
        deadlineUnixMillis:
          remainingMs === undefined ? 0 : Date.now() + Math.max(0, Math.ceil(remainingMs)),
        samplingEnabled: context.samplingEnabled(),
        traceCarrier: Object.fromEntries(context.transportMetadata()),
        scheduled: false,
        scheduleId: "",
        scheduledAtUnixMillis: 0,
        firedAtUnixMillis: 0,
        payload: this.stream.inputSerde().serialize(value)
      };
      const result = await this.connector.submitEndpoint(
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
      this.sinkEndpoint.onRequestEnd(context, started, failure);
      span?.end();
    }
  }
}

export function makeTemporalSinkEndpointConsumer<T, E>(stream: TypedSinkStream<T, E>): Consumer<T> {
  return makeSinkConsumer<T, never, E>(stream, false);
}

export function makeTemporalSinkEndpointConsumerWithResult<T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>
): Consumer<T> {
  return makeSinkConsumer(stream, true);
}

function makeSinkConsumer<T, R, E>(
  stream: TypedSinkStream<T, E> | TypedSinkStreamWithResult<T, R, E>,
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
  const consumer = new TemporalSinkConsumer(endpoint, connector, stream, withResult);
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
