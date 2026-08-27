import {
  applyDataSourceEndpointTracing,
  DataConnectorType,
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  FunctionCollector,
  InputDataSource,
  ScheduleBackend,
  bindDurableCallSpan,
  errorFromUnknown,
  makeScheduleTrigger,
  makeStreamContext,
  newStreamId,
  spanError,
  stringAttribute,
  type Completion,
  type Context,
  type Consumer,
  type MessageContext,
  type RuntimeEnvironment,
  type ScheduleEndpointFunction,
  type ScheduleTrigger,
  type Span,
  type StreamContext,
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

export interface TemporalEndpointHandler<State, Input, T, R, E> {
  beginRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>
  ):
    | { readonly context: MessageContext; readonly state: State }
    | Promise<{ readonly context: MessageContext; readonly state: State }>;
  consumeMessage(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    state: State,
    value: Readonly<Input>
  ): Completion;
  endRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    error: Error | undefined,
    state: State
  ): Completion;
}

class DirectTemporalEndpointHandler<T, R, E> implements TemporalEndpointHandler<
  undefined,
  T,
  T,
  R,
  E
> {
  public beginRequest(context: MessageContext): {
    readonly context: MessageContext;
    readonly state: undefined;
  } {
    return { context, state: undefined };
  }

  public consumeMessage(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    _state: undefined,
    value: Readonly<T>
  ): Completion {
    return stream.collect(context, value);
  }

  public endRequest(): void {}
}

class TemporalEndpointConsumer<State, Input, T, R, E> extends DataSourceEndpointConsumer<T, R, E> {
  readonly #endpoint: DataSourceEndpoint;
  readonly #stream: TypedInputStream<T, R, E>;
  readonly #decode: (envelope: EndpointEnvelope) => Input;
  readonly #handler: TemporalEndpointHandler<State, Input, T, R, E>;
  readonly #streamContext: StreamContext<T, R, E>;
  readonly #pending = new Map<string, PendingResult<R>>();
  readonly #tracer: Tracer | undefined;

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    connector: TemporalConnector,
    decode: (envelope: EndpointEnvelope) => Input,
    handler: TemporalEndpointHandler<State, Input, T, R, E>
  ) {
    super(endpoint, stream);
    this.#endpoint = endpoint;
    this.#stream = stream;
    this.#decode = decode;
    this.#handler = handler;
    this.#streamContext = makeStreamContext(
      stream,
      stream.resultStream(),
      new FunctionCollector((context, value: T) => stream.consume(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
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
    connector.registerEndpoint(endpoint.id, (envelope, context, cancellationSignal) =>
      this.activate(envelope, context, cancellationSignal)
    );
  }

  public async activate(
    envelope: EndpointEnvelope,
    parent: MessageContext,
    cancellationSignal?: AbortSignal
  ): Promise<EndpointResult> {
    if (envelope.version !== 1 || envelope.endpointId !== this.#endpoint.id) {
      throw new Error(`invalid Temporal endpoint envelope for ${this.#endpoint.name}`);
    }
    let context = parent
      .withStreamId(envelope.streamId || newStreamId())
      .withPriority(envelope.priority);
    context = applyDataSourceEndpointTracing(
      context,
      this.#stream.runtimeEnvironment(),
      this.#endpoint.id
    );
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
    const startedHandler = await this.#handler.beginRequest(context, this.#streamContext);
    context = startedHandler.context;
    const state = startedHandler.state;
    const started = this.#endpoint.onRequestStart(context);
    const resultStream = this.#stream.resultStream();
    let pending: PendingResult<R> | undefined;
    let failure: Error | undefined;
    try {
      if (resultStream !== undefined) {
        if (this.#pending.has(streamId)) {
          throw new Error(`Temporal execution ${streamId} is already active`);
        }
        pending = { ...Promise.withResolvers<R>(), settled: false };
        this.#pending.set(streamId, pending);
        this.#endpoint.onPendingAdd(context, streamId);
      }
      await this.#handler.consumeMessage(
        context,
        this.#streamContext,
        state,
        this.#decode(envelope)
      );
      if (pending === undefined) return { payload: new Uint8Array() };
      const value = await pending.promise;
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
      await this.#handler.endRequest(context, this.#streamContext, failure, state);
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
  return makeTemporalEndpointConsumerWithHandler(
    stream,
    new DirectTemporalEndpointHandler<T, R, E>()
  );
}

export function makeTemporalEndpointConsumerWithHandler<State, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  handler: TemporalEndpointHandler<State, T, T, R, E>
): Consumer<T> {
  return makeEndpointConsumer(
    stream,
    (envelope) => stream.serde().deserialize(envelope.payload),
    handler
  );
}

export function makeTemporalScheduleEndpointConsumer<T, R, E>(
  stream: TypedInputStream<T, R, E>,
  function_: ScheduleEndpointFunction<T>
): Consumer<T> {
  type Activation =
    | { readonly scheduled: true; readonly trigger: ScheduleTrigger }
    | { readonly scheduled: false; readonly value: T };
  return makeEndpointConsumer(
    stream,
    (envelope): Activation => {
      if (!envelope.scheduled) {
        return { scheduled: false, value: stream.serde().deserialize(envelope.payload) };
      }
      if (
        envelope.scheduleId === "" ||
        envelope.scheduledAtUnixMillis <= 0 ||
        envelope.firedAtUnixMillis <= 0
      ) {
        throw new Error(`invalid Temporal schedule envelope for ${String(envelope.endpointId)}`);
      }
      return {
        scheduled: true,
        trigger: makeScheduleTrigger(
          envelope.endpointId,
          envelope.scheduleId,
          new Date(envelope.scheduledAtUnixMillis).toISOString(),
          new Date(envelope.firedAtUnixMillis).toISOString(),
          ScheduleBackend.Temporal
        )
      };
    },
    {
      beginRequest: (context) => ({ context, state: undefined }),
      consumeMessage: (context, streamContext, _state, activation) =>
        activation.scheduled
          ? function_.onTrigger(
              context,
              activation.trigger,
              new FunctionCollector((nextContext, value) =>
                streamContext.collect(nextContext, value)
              )
            )
          : streamContext.collect(context, activation.value),
      endRequest: () => undefined
    }
  );
}

function makeEndpointConsumer<State, Input, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  decode: (envelope: EndpointEnvelope) => Input,
  handler: TemporalEndpointHandler<State, Input, T, R, E>
): Consumer<T> {
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
  const consumer = new TemporalEndpointConsumer(endpoint, stream, connector, decode, handler);
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
