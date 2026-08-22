import {
  DataConnectorType,
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  FunctionCollector,
  InputDataSource,
  Context,
  RotatingMap,
  RuntimeTaskRegistry,
  err,
  errorFromUnknown,
  makeStreamContext,
  newStreamId,
  spanError,
  str,
  stringAttribute,
  type Completion,
  type Consumer,
  type InputEndpointConsumer,
  type MessageContext,
  type RuntimeEnvironment,
  type Span,
  type StreamContext,
  type Tracer,
  type TypedInputStream
} from "../../runtime/index.js";

const PENDING_ROTATION_INTERVAL_MS = 30_000;

export interface DataProducer<T> {
  start(context: Context, consumer: Consumer<T>): Completion;
  stop(context: Context): Completion;
}

export type ResultCallback<HandlerState, T, R, E> = (
  context: MessageContext,
  stream: StreamContext<T, R, E>,
  handlerState: HandlerState,
  value: Readonly<R>
) => boolean | Promise<boolean>;

export interface ResultContext<HandlerState, T, R, E> {
  setResultCallback(messageId: string, callback: ResultCallback<HandlerState, T, R, E>): void;
  done(): void;
}

export interface EndpointHandler<HandlerState, T, R, E> {
  concurrency(stream: StreamContext<T, R, E>): number;
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
    result: ResultContext<HandlerState, T, R, E>
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
    handlerState: HandlerState
  ): Completion;
}

class CustomResult<HandlerState, T, R, E> implements ResultContext<HandlerState, T, R, E> {
  public readonly state: HandlerState;
  readonly #callbacks = new Map<string, ResultCallback<HandlerState, T, R, E>>();
  readonly #done: Promise<void>;
  #resolveDone: (() => void) | undefined;
  #completed = false;
  #retiring = false;
  #activeCallbacks = 0;
  #retired: Promise<void> | undefined;
  #resolveRetired: (() => void) | undefined;

  public constructor(state: HandlerState) {
    this.state = state;
    this.#done = new Promise((resolve) => {
      this.#resolveDone = resolve;
    });
  }

  public setResultCallback(
    messageId: string,
    callback: ResultCallback<HandlerState, T, R, E>
  ): void {
    this.#callbacks.set(messageId, callback);
  }

  public callback(messageId: string): ResultCallback<HandlerState, T, R, E> | undefined {
    return this.#callbacks.get(messageId);
  }

  public remove(messageId: string, callback: ResultCallback<HandlerState, T, R, E>): boolean {
    if (this.#callbacks.get(messageId) !== callback) return false;
    return this.#callbacks.delete(messageId);
  }

  public done(): void {
    if (this.#completed) return;
    this.#completed = true;
    this.#resolveDone?.();
    this.#resolveDone = undefined;
  }

  public wait(): Promise<void> {
    return this.#done;
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

interface CustomEndpointConsumerContract<T> extends InputEndpointConsumer {
  start(context: Context): Promise<void>;
  stop(context: Context): Promise<void>;
  handle(context: MessageContext, value: T): Promise<void>;
}

class CustomSourceEndpoint<T> extends DataSourceEndpoint implements Consumer<T> {
  readonly #producer: DataProducer<T>;
  readonly #producerTasks: RuntimeTaskRegistry;
  #binding: CustomEndpointConsumerContract<T> | undefined;
  #started = false;

  public constructor(dataSource: CustomDataSource, endpointId: number, producer: DataProducer<T>) {
    super(dataSource, endpointId);
    this.#producer = producer;
    this.#producerTasks = new RuntimeTaskRegistry((error) => {
      this.runtimeEnvironment()
        .log()
        .error(Context.background(), "data producer error", str("endpoint", this.name), err(error));
    });
  }

  public bind(binding: CustomEndpointConsumerContract<T>): void {
    if (this.#binding !== undefined) {
      throw new Error(`consumer already assigned to custom endpoint ${this.name}`);
    }
    this.#binding = binding;
    this.addEndpointConsumer(binding);
  }

  public consume(context: MessageContext, value: T): Completion {
    return this.#binding?.handle(context, value);
  }

  public async start(context: Context): Promise<void> {
    if (this.#started) throw new Error(`custom endpoint ${this.name} is already started`);
    const binding = this.#binding;
    if (binding === undefined) throw new Error(`custom endpoint ${this.name} has no consumer`);
    await binding.start(context);
    this.#started = true;
    this.#producerTasks.admitDetached(async (signal) => {
      await this.#producer.start(context.withExternalCancellation(signal), this);
    });
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) return;
    this.#started = false;
    await this.#binding?.stop(context);
    this.#producerTasks.cancel(new Error(`custom endpoint ${this.name} stopped`));
    try {
      await this.#producer.stop(context);
    } finally {
      await this.#producerTasks.drain(context.remainingMs());
    }
  }
}

export class CustomDataSource extends InputDataSource {
  #started = false;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    if (this.config().type !== DataConnectorType.Custom) {
      throw new Error(`data source ${this.name} is not custom`);
    }
  }

  public async start(context: Context): Promise<void> {
    if (this.#started) throw new Error(`custom data source ${this.name} is already started`);
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

  private customEndpoints(): readonly CustomSourceEndpoint<unknown>[] {
    return this.endpoints().map((endpoint) => {
      if (!(endpoint instanceof CustomSourceEndpoint)) {
        throw new Error(`source endpoint ${endpoint.name} is not custom`);
      }
      return endpoint;
    });
  }
}

class CustomEndpointConsumer<HandlerState, T, R, E>
  extends DataSourceEndpointConsumer<T, R, E>
  implements CustomEndpointConsumerContract<T>
{
  readonly #streamContext: StreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, T, R, E>;
  readonly #tasks = new RuntimeTaskRegistry();
  readonly #waiters: (() => void)[] = [];
  readonly #tracer: Tracer | undefined;
  #pending: RotatingMap<string, CustomResult<HandlerState, T, R, E>> | undefined;
  #active = 0;
  #started = false;
  #stopped = false;

  public constructor(
    endpoint: CustomSourceEndpoint<T>,
    stream: TypedInputStream<T, R, E>,
    handler: EndpointHandler<HandlerState, T, R, E>
  ) {
    super(endpoint, stream);
    this.#handler = handler;
    this.#streamContext = makeStreamContext(
      stream,
      stream.resultStream(),
      new FunctionCollector((context, value: T) => stream.consume(context, value)),
      new FunctionCollector((context, value: E) => stream.errorStream().consume(context, value))
    );
    if (stream.resultStream() !== undefined) {
      stream.setResultConsumer({
        consume: (context, value) => this.consumeResult(context, value)
      });
    }
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public start(context: Context): Promise<void> {
    if (this.#started) {
      return Promise.reject(new Error(`custom endpoint ${this.endpoint().name} already started`));
    }
    this.#started = true;
    this.#stopped = false;
    if (this.stream().resultStream() !== undefined) {
      this.#pending = new RotatingMap(PENDING_ROTATION_INTERVAL_MS);
      this.#pending.start(context);
    }
    return Promise.resolve();
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) return;
    this.#started = false;
    this.#stopped = true;
    for (const wake of this.#waiters.splice(0)) wake();
    this.#tasks.cancel(context.signal().reason ?? new Error("custom endpoint stopped"));
    try {
      await this.#tasks.drain(context.remainingMs());
    } finally {
      this.#pending?.stop(context);
    }
  }

  public override consume(context: MessageContext, value: T): Completion {
    return this.stream().consume(context, value);
  }

  public handle(context: MessageContext, value: T): Promise<void> {
    if (!this.#started) return Promise.resolve();
    return this.#tasks.admit(
      async (signal) => this.handleOnce(context, value, signal),
      context.signal()
    );
  }

  private async handleOnce(context: MessageContext, value: T, signal: AbortSignal): Promise<void> {
    await this.acquire(signal);
    try {
      await this.handleAdmitted(context.withExternalCancellation(signal), value);
    } finally {
      this.#active -= 1;
      this.#waiters.shift()?.();
    }
  }

  private async handleAdmitted(context: MessageContext, value: T): Promise<void> {
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "local.input", [
        stringAttribute("stream", this.stream().name),
        stringAttribute("endpoint", this.endpoint().name)
      ]);
      context = started.context;
      span = started.span;
    }
    try {
      await this.handleTraced(context, value, span);
    } finally {
      span?.end();
    }
  }

  private async handleTraced(
    context: MessageContext,
    value: T,
    span: Span | undefined
  ): Promise<void> {
    let state: HandlerState;
    try {
      const started = await this.#handler.beginRequest(context, this.#streamContext);
      context = started.context;
      state = started.state;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      this.endpoint().onBeginRequestFailed(context, failure);
      return;
    }
    span?.addEvent("begin_request");
    const startedAt = this.endpoint().onRequestStart(context);
    const streamId = context.streamId() ?? newStreamId();
    context = context.withStreamId(streamId);
    const result = new CustomResult<HandlerState, T, R, E>(state);
    const hasResult = this.stream().resultStream() !== undefined;
    if (hasResult) {
      try {
        this.pending().set(streamId, result);
        this.endpoint().onPendingAdd(context, streamId);
      } catch (error: unknown) {
        const failure = errorFromUnknown(error);
        spanError(span, failure);
        await this.#handler.endRequest(context, this.#streamContext, failure, state);
        this.endpoint().onRequestEnd(context, startedAt, failure);
        return;
      }
    }
    let failure: Error | undefined;
    let resultWaitFailed = false;
    try {
      await this.#handler.consumeMessage(context, this.#streamContext, state, value, result);
      span?.addEvent("consume_message");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
    }
    if (failure === undefined && hasResult) {
      try {
        await waitForResult(result, context.signal());
        span?.addEvent("done_received");
      } catch (error: unknown) {
        failure = errorFromUnknown(error);
        resultWaitFailed = true;
      }
    }
    if (hasResult) {
      const resultCompleted = await result.retire();
      if (resultWaitFailed && resultCompleted) {
        failure = undefined;
        span?.addEvent("done_received");
      }
      this.pending().pop(streamId);
      this.endpoint().onPendingRemove(context, streamId);
    }
    if (failure !== undefined) {
      spanError(span, failure);
      if (context.cancelled()) {
        span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
      }
    }
    try {
      await this.#handler.endRequest(context, this.#streamContext, failure, state);
    } catch (error: unknown) {
      failure ??= errorFromUnknown(error);
      spanError(span, failure);
    } finally {
      this.endpoint().onRequestEnd(context, startedAt, failure);
    }
  }

  private async acquire(signal: AbortSignal): Promise<void> {
    for (;;) {
      if (this.#stopped || signal.aborted) {
        throw signal.reason === undefined
          ? new Error("custom endpoint stopped")
          : errorFromUnknown(signal.reason);
      }
      const concurrency = this.#handler.concurrency(this.#streamContext);
      if (concurrency < 0 || !Number.isSafeInteger(concurrency)) {
        throw new RangeError("custom endpoint concurrency must be a non-negative safe integer");
      }
      if (concurrency === 0 || this.#active < concurrency) {
        this.#active += 1;
        return;
      }
      await new Promise<void>((resolve) => this.#waiters.push(resolve));
    }
  }

  private async consumeResult(context: MessageContext, value: R): Promise<void> {
    const streamId = context.streamId();
    if (streamId === undefined) {
      this.endpoint().onMissingStreamId(context);
      return;
    }
    const [result, found] = this.pending().get(streamId);
    if (!found || result?.beginCallback() !== true) {
      this.endpoint().onLateResult(context, streamId);
      return;
    }
    try {
      const messageId = this.#handler.getMessageId(
        context,
        this.#streamContext,
        result.state,
        value
      );
      const callback = result.callback(messageId);
      if (callback === undefined) {
        this.endpoint().onUnknownMessageId(context, streamId, messageId);
        return;
      }
      if (await callback(context, this.#streamContext, result.state, value)) {
        if (!result.remove(messageId, callback)) {
          this.endpoint().onDuplicateMessageId(context, streamId, messageId);
        }
      }
    } finally {
      result.endCallback();
    }
  }

  private pending(): RotatingMap<string, CustomResult<HandlerState, T, R, E>> {
    if (this.#pending === undefined) {
      throw new Error(`custom endpoint ${this.endpoint().name} pending store is not started`);
    }
    return this.#pending;
  }
}

export function makeCustomEndpointConsumer<HandlerState, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  producer: DataProducer<T>,
  handler: EndpointHandler<HandlerState, T, R, E>
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
  if (endpointConfig === undefined) {
    throw new Error(`custom endpoint config ${String(stream.endpointId())} not found`);
  }
  const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
  if (dataSource.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new CustomSourceEndpoint(dataSource, endpointConfig.id, producer);
  const consumer = new CustomEndpointConsumer(endpoint, stream, handler);
  endpoint.bind(consumer);
  dataSource.addEndpoint(endpoint);
  return consumer;
}

function getOrCreateDataSource(
  connectorId: number,
  environment: RuntimeEnvironment
): CustomDataSource {
  const existing = environment.dataSourceById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof CustomDataSource)) {
      throw new Error(`data source ${String(connectorId)} is not custom`);
    }
    return existing;
  }
  const dataSource = new CustomDataSource(connectorId, environment);
  environment.addDataSource(dataSource);
  return dataSource;
}

async function waitForResult<HandlerState, T, R, E>(
  result: CustomResult<HandlerState, T, R, E>,
  signal: AbortSignal
): Promise<void> {
  if (signal.aborted) {
    throw signal.reason === undefined
      ? new Error("custom source request cancelled")
      : errorFromUnknown(signal.reason);
  }
  let cancelled: (() => void) | undefined;
  try {
    await Promise.race([
      result.wait(),
      new Promise<never>((_resolve, reject) => {
        cancelled = () => {
          reject(
            signal.reason === undefined
              ? new Error("custom source request cancelled")
              : errorFromUnknown(signal.reason)
          );
        };
        signal.addEventListener("abort", cancelled, { once: true });
      })
    ]);
  } finally {
    if (cancelled !== undefined) signal.removeEventListener("abort", cancelled);
  }
}
