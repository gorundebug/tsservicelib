import {
  applyDataSourceEndpointTracing,
  DataConnectorType,
  DataSourceEndpoint,
  FunctionCollector,
  InputDataSource,
  MessageContext,
  ScheduleBackend,
  bindDurableCallSpan,
  errorFromUnknown,
  makeScheduleTrigger,
  newStreamId,
  spanError,
  stringAttribute,
  type Completion,
  type Context,
  type Consumer,
  type DurableCallContext,
  type InputEndpoint,
  type InputEndpointConsumer,
  type RuntimeEnvironment,
  type ScheduleEndpointFunction,
  type ScheduleTrigger,
  type Span,
  type Tracer,
  type TypedInputStream
} from "../../runtime/index.js";
import { makeTemporalConnector, type TemporalConnector } from "./connector.js";
import type { EndpointEnvelope, EndpointResult } from "./contracts.js";

class TemporalDataSource extends InputDataSource {
  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    if (this.config().type !== DataConnectorType.Temporal) {
      throw new Error(`data source ${this.name} is not Temporal`);
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

interface PendingResult<R> {
  readonly promise: Promise<R>;
  readonly resolve: (value: R) => void;
  settled: boolean;
}

class TemporalEndpointConsumer<Input, T, R, E> implements InputEndpointConsumer, Consumer<Input> {
  readonly #endpoint: DataSourceEndpoint;
  readonly #stream: TypedInputStream<T, R, E>;
  readonly #decode: (envelope: EndpointEnvelope) => Input;
  readonly #activateInput: (context: MessageContext, value: Input) => Completion;
  readonly #pending = new Map<string, PendingResult<R>>();
  readonly #tracer: Tracer | undefined;

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    connector: TemporalConnector,
    decode: (envelope: EndpointEnvelope) => Input,
    activateInput: (context: MessageContext, value: Input) => Completion
  ) {
    this.#endpoint = endpoint;
    this.#stream = stream;
    this.#decode = decode;
    this.#activateInput = activateInput;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    if (stream.resultStream() !== undefined) {
      stream.setResultConsumer({
        consume: (context, value) => {
          this.consumeResult(context, value);
        }
      });
    }
    connector.registerEndpoint(endpoint.id, (envelope, cancellationSignal, durableCallContext) =>
      this.activate(envelope, cancellationSignal, durableCallContext)
    );
  }

  public endpoint(): InputEndpoint {
    return this.#endpoint;
  }

  public consume(context: MessageContext, value: Input): Completion {
    return this.#activateInput(context, value);
  }

  public async activate(
    envelope: EndpointEnvelope,
    cancellationSignal?: AbortSignal,
    durableCallContext?: DurableCallContext
  ): Promise<EndpointResult> {
    if (envelope.version !== 1 || envelope.endpointId !== this.#endpoint.id) {
      throw new Error(`invalid Temporal endpoint envelope for ${this.#endpoint.name}`);
    }
    let context = new MessageContext()
      .withMetadata(new Map(Object.entries(envelope.traceCarrier)))
      .withStreamId(envelope.streamId || newStreamId())
      .withPriority(envelope.priority)
      .withSampling(envelope.samplingEnabled);
    context = applyDataSourceEndpointTracing(
      context,
      this.#stream.runtimeEnvironment(),
      this.#endpoint.id
    );
    if (durableCallContext !== undefined) {
      context = context.withDurableCallContext(durableCallContext);
    }
    if (cancellationSignal !== undefined) {
      context = context.withExternalCancellation(cancellationSignal);
    }
    if (envelope.deadlineUnixMillis > 0) {
      context = context.bounded(Math.max(0, envelope.deadlineUnixMillis - Date.now()));
    }
    const streamId = context.streamId();
    if (streamId === undefined) throw new Error("Temporal endpoint stream ID was not created");
    let span: Span | undefined;
    let durableSpan = false;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const startedSpan = this.#tracer.start(context, "temporal.input", [
        stringAttribute("stream", this.#stream.name),
        stringAttribute("endpoint", this.#endpoint.name)
      ]);
      context = startedSpan.context;
      span = startedSpan.span;
      durableSpan = bindDurableCallSpan(context, span);
    }
    const started = this.#endpoint.onRequestStart(context);
    const expectsResult = this.#stream.resultStream() !== undefined;
    let pending: PendingResult<R> | undefined;
    let failure: Error | undefined;
    try {
      if (expectsResult) {
        if (this.#pending.has(streamId)) {
          throw new Error(`Temporal execution ${streamId} is already active`);
        }
        pending = { ...Promise.withResolvers<R>(), settled: false };
        this.#pending.set(streamId, pending);
        this.#endpoint.onPendingAdd(context, streamId);
      }
      await this.consume(context, this.#decode(envelope));
      if (pending === undefined) return { payload: new Uint8Array() };
      const value = await pending.promise;
      const resultStream = this.#stream.resultStream();
      if (resultStream === undefined)
        throw new Error("Temporal endpoint result stream disappeared");
      return { payload: resultStream.serde().serialize(value) };
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      spanError(span, failure);
      throw failure;
    } finally {
      if (pending !== undefined) {
        this.#pending.delete(streamId);
        this.#endpoint.onPendingRemove(context, streamId);
      }
      this.#endpoint.onRequestEnd(context, started, failure);
      if (!durableSpan) span?.end();
    }
  }

  private consumeResult(context: MessageContext, value: R): void {
    const streamId = context.streamId();
    if (streamId === undefined) {
      this.#endpoint.onMissingStreamId(context);
      return;
    }
    const pending = this.#pending.get(streamId);
    if (pending === undefined) {
      this.#endpoint.onLateResult(context, streamId);
      return;
    }
    if (pending.settled) {
      this.#endpoint.onDuplicateMessageId(context, streamId, streamId);
      return;
    }
    pending.settled = true;
    pending.resolve(value);
  }
}

export function makeTemporalEndpointConsumer<T, R, E>(
  stream: TypedInputStream<T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(
    stream,
    (envelope) => stream.serde().deserialize(envelope.payload),
    (context, value) => stream.consume(context, value)
  );
}

export function makeTemporalScheduleEndpointConsumer<T, R, E>(
  stream: TypedInputStream<T, R, E>,
  function_: ScheduleEndpointFunction<T>
): Consumer<ScheduleTrigger> {
  const collector = new FunctionCollector<T>((context, value) => stream.consume(context, value));
  return makeEndpointConsumer(
    stream,
    (envelope) => {
      if (
        !envelope.scheduled ||
        envelope.scheduleId === "" ||
        envelope.scheduledAtUnixMillis <= 0 ||
        envelope.firedAtUnixMillis <= 0
      ) {
        throw new Error(`invalid Temporal schedule envelope for ${String(envelope.endpointId)}`);
      }
      return makeScheduleTrigger(
        envelope.endpointId,
        envelope.scheduleId,
        new Date(envelope.scheduledAtUnixMillis).toISOString(),
        new Date(envelope.firedAtUnixMillis).toISOString(),
        ScheduleBackend.Temporal
      );
    },
    (context, trigger) => function_.onTrigger(context, trigger, collector)
  );
}

function makeEndpointConsumer<Input, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  decode: (envelope: EndpointEnvelope) => Input,
  activateInput: (context: MessageContext, value: Input) => Completion
): Consumer<Input> {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
  if (endpointConfig === undefined) {
    throw new Error(`Temporal endpoint config ${String(stream.endpointId())} not found`);
  }
  const connector = makeTemporalConnector(endpointConfig.idDataConnector, environment);
  const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
  if (dataSource.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`Temporal endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new DataSourceEndpoint(dataSource, endpointConfig.id);
  const consumer = new TemporalEndpointConsumer(endpoint, stream, connector, decode, activateInput);
  endpoint.addEndpointConsumer(consumer);
  dataSource.addEndpoint(endpoint);
  return consumer;
}

function getOrCreateDataSource(
  connectorId: number,
  environment: RuntimeEnvironment
): TemporalDataSource {
  const existing = environment.dataSourceById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof TemporalDataSource)) {
      throw new Error(`data source ${String(connectorId)} is not Temporal`);
    }
    return existing;
  }
  const dataSource = new TemporalDataSource(connectorId, environment);
  environment.addDataSource(dataSource);
  return dataSource;
}
