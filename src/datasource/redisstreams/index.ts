import { SourceMetrics } from "../../internal/broker-source-metrics.js";
import {
  InputDataSource,
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  MessageContext,
  MessageContextKey,
  FunctionCollector,
  makeStreamContext,
  newStreamId,
  applyDataSourceEndpointTracing,
  requireRedisStreamsDataConnectorConfig,
  requireRedisStreamsEndpointConfig,
  validateRedisStreamsEndpoint,
  errorFromUnknown,
  err,
  type Context,
  type Completion,
  type Consumer,
  type RuntimeEnvironment,
  type StreamContext,
  type TypedInputStream
} from "../../runtime/index.js";
import {
  cancellation,
  Session,
  type Entry,
  entryMetadata,
  BYTE_LIMIT,
  MESSAGE_LIMIT,
  DeletedPendingEntriesError,
  wait,
  PENDING_MESSAGES
} from "../../internal/redis/client.js";

const settlementContexts = new WeakMap<ConsumerMessage, Context>();

/** Explicit native XACK; returns the server's affected PEL entry count. */
export class ConsumerMessage {
  #active = true;
  #settled = false;
  public constructor(
    public readonly message: Entry,
    private readonly session: Session,
    private readonly epoch: number,
    private readonly context: Context
  ) {}
  public retire(): void {
    this.#active = false;
  }
  public async ack(): Promise<number> {
    if (!this.#active || this.#settled)
      throw new Error("RedisStreams ACK handle expired or already used");
    this.#settled = true;
    return this.session.ack(
      this.message.id,
      this.epoch,
      settlementContexts.get(this) ?? this.context
    );
  }
}
export type ResultCallback<H, T, R, E> = (
  context: MessageContext,
  stream: StreamContext<T, R, E>,
  state: H,
  value: Readonly<R>
) => boolean | Promise<boolean>;
export interface ResultContext<H, T, R, E> {
  setResultCallback(id: string, callback: ResultCallback<H, T, R, E>): void;
  /** Local lifecycle completion only, never ACK. Register callbacks before collect. */
  done(): void;
}
export interface EndpointHandler<H, T, R, E> {
  onError(context: MessageContext, stream: StreamContext<T, R, E>, error: Error): Completion;
  beginRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>
  ): { context: MessageContext; state: H } | Promise<{ context: MessageContext; state: H }>;
  consumeMessage(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    state: H,
    message: ConsumerMessage,
    result: ResultContext<H, T, R, E>
  ): Completion;
  getMessageId(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    state: H,
    value: Readonly<R>
  ): string;
  endRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    error: Error | undefined,
    state: H
  ): Completion;
}

const requestIdentity = new MessageContextKey<string | undefined>(undefined);

class Request<H, T, R, E> implements ResultContext<H, T, R, E> {
  public readonly identity = newStreamId();
  public completed = false;
  public retired = false;
  public readonly callbacks = new Map<string, ResultCallback<H, T, R, E>>();
  public readonly active = new Set<Promise<void>>();
  public readonly completion: Promise<void>;
  #finish!: () => void;
  public constructor(public readonly state: H) {
    this.completion = new Promise((resolve) => {
      this.#finish = resolve;
    });
  }
  public done(): void {
    this.completed = true;
    this.#finish();
  }
  public setResultCallback(id: string, callback: ResultCallback<H, T, R, E>): void {
    if (this.retired) throw new Error("RedisStreams request already retired");
    if (this.callbacks.size >= PENDING_MESSAGES)
      throw new Error("RedisStreams result callback limit exceeded");
    this.callbacks.set(id, callback);
  }
}

class SourceEndpoint extends DataSourceEndpoint {
  public readonly observations = new SourceMetrics(this.runtimeEnvironment().metrics(), "redisstreams", this.dataSource().name, this.name);
  public handle!: (context: Context, message: ConsumerMessage) => Promise<void>;
  public report!: (context: Context, error: Error) => Promise<void>;
  #session: Session | undefined;
  #controller = new AbortController();
  #generation = new AbortController();
  #generationContext!: Context;
  #started = false;
  #stopping = false;
  #tasks = new Set<Promise<void>>();
  #bytes = 0;
  #run: Promise<void> = Promise.resolve();
  #reporting: Promise<void> | undefined;
  public async start(parent: Context): Promise<void> {
    if (this.#started || this.#stopping)
      throw new Error("RedisStreams source already started/stopped");
    this.#started = true;
    const ep = requireRedisStreamsEndpointConfig(this.config());
    if (!ep.enabled) return;
    const dc = requireRedisStreamsDataConnectorConfig(this.dataSource().config());
    validateRedisStreamsEndpoint(ep, false);
    const owner = parent.withExternalCancellation(this.#controller.signal);
    // One cancellation join per transport generation, not per delivered record.
    this.#generationContext = owner.withExternalCancellation(this.#generation.signal);
    const session = new Session(dc, ep, true, (error) => {
      this.#generation.abort(error);
      this.#generation = new AbortController();
      this.#generationContext = owner.withExternalCancellation(this.#generation.signal);
      if (!this.#reporting)
        this.#reporting = this.report(owner, error).finally(() => {
          this.#reporting = undefined;
        });
      else
        this.runtimeEnvironment().log().error(owner, "RedisStreams transport failure", err(error));
    });
    this.#session = session;
    await session.start(owner);
    this.#run = this.run(owner, session).catch((error: unknown) =>
      this.report(owner, errorFromUnknown(error))
    );
  }
  private async run(owner: Context, session: Session): Promise<void> {
    let cursor = "0-0",
      nextClaim = 0;
    while (!this.stopping() && !owner.cancelled() && !session.stopped) {
      if (!session.isReady()) {
        await wait(new Promise<void>((resolve) => setTimeout(resolve, 25)), owner);
        continue;
      }
      const free = Math.min(
        session.limit - this.#tasks.size,
        Math.floor((BYTE_LIMIT - this.#bytes) / MESSAGE_LIMIT)
      );
      if (free < 1) {
        await wait(Promise.race(this.#tasks), owner);
        continue;
      }
      try {
        let messages: Entry[];
        const epoch = session.epoch;
        if (performance.now() >= nextClaim) {
          const claimed = await session.claim(cursor, free, owner);
          for (const _entry of claimed.entries) this.observations.observe(owner, "reclaimed");
          // Advance even on an empty page: Redis bounds the scan independently of claims.
          cursor = claimed.cursor;
          nextClaim = performance.now() + (session.ep.claimIntervalMs ?? 1000);
          if (claimed.deleted.length)
            await this.report(owner, new DeletedPendingEntriesError(claimed.deleted));
          messages = claimed.entries;
        } else
          messages = await session.read(
            free,
            owner,
            Math.min(
              session.ep.readBlockMs ?? 1000,
              Math.max(1, Math.ceil(nextClaim - performance.now()))
            )
          );
        if (this.stopping() || epoch !== session.epoch) continue; // Remain pending for reclaim, never ACK.
        for (const entry of messages) {
          const size = entry.bytes;
          if (
            size > MESSAGE_LIMIT ||
            this.#bytes + size > BYTE_LIMIT ||
            this.#tasks.size >= session.limit
          ) {
            await this.report(
              owner,
              new Error(`RedisStreams entry ${entry.id} exceeds bounded admission; left pending`)
            );
            continue;
          }
          this.#bytes += size;
          const context = this.#generationContext;
          const message = new ConsumerMessage(entry, session, epoch, context);
          const task = Promise.resolve()
            .then(() => this.handle(context, message))
            .catch((error: unknown) => this.report(context, errorFromUnknown(error)))
            .finally(() => {
              message.retire();
              this.#bytes -= size;
            });
          this.#tasks.add(task);
          void task.then(
            () => this.#tasks.delete(task),
            () => this.#tasks.delete(task)
          );
        }
      } catch (error) {
        if (this.stopping() || owner.cancelled()) break;
        if (session.isReady()) {
          await this.report(owner, errorFromUnknown(error));
          await wait(
            new Promise<void>((resolve) => setTimeout(resolve, session.ep.claimIntervalMs ?? 1000)),
            owner
          );
        }
      }
    }
  }
  private stopping(): boolean {
    return this.#stopping;
  }
  public stopInput(): void {
    this.#stopping = true;
    this.#session?.pause();
  }
  public async stop(context: Context): Promise<void> {
    this.stopInput();
    try {
      await wait(
        (async () => {
          await this.#run;
          await Promise.allSettled([...this.#tasks, ...(this.#reporting ? [this.#reporting] : [])]);
        })(),
        context
      );
    } finally {
      this.#session?.close();
      this.#controller.abort(new Error("RedisStreams source stopped"));
    }
  }
}

export class RedisStreamsDataSource extends InputDataSource {
  public constructor(id: number, environment: RuntimeEnvironment) {
    super(id, environment);
  }
  public async start(context: Context): Promise<void> {
    try {
      for (const ep of this.endpoints()) await (ep as SourceEndpoint).start(context);
    } catch (error) {
      await this.stop(context.bounded(1000)).catch(() => undefined);
      throw error;
    }
  }
  public async stop(context: Context): Promise<void> {
    for (const ep of this.endpoints()) (ep as SourceEndpoint).stopInput();
    const results = await Promise.allSettled(
      this.endpoints().map((ep) => (ep as SourceEndpoint).stop(context))
    );
    const failure = results.find((result) => result.status === "rejected");
    if (failure?.status === "rejected") throw errorFromUnknown(failure.reason);
  }
}

class EndpointConsumer<H, T, R, E> extends DataSourceEndpointConsumer<T, R, E> {
  #pending = new Map<string, Request<H, T, R, E>>();
  readonly #sc: StreamContext<T, R, E>;
  public constructor(
    private readonly ep: SourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    private readonly handler: EndpointHandler<H, T, R, E>
  ) {
    super(ep, stream);
    this.#sc = makeStreamContext(
      stream,
      stream.resultStream(),
      new FunctionCollector((c, v: T) => stream.consume(c, v)),
      new FunctionCollector((c, v: E) => stream.errorStream().consume(c, v))
    );
    stream.setResultConsumer({ consume: (context, value) => this.result(context, value) });
    ep.handle = (context, message) => {
      ep.observations.observe(context, "received");
      
      return this.handle(context, message);
    };
    ep.report = async (context, error) => {
      try {
        await handler.onError(MessageContext.fromContext(context), this.#sc, error);
      } catch (failure) {
        ep.runtimeEnvironment()
          .log()
          .error(context, "RedisStreams onError failed", err(errorFromUnknown(failure)));
      }
    };
  }
  private async handle(owner: Context, message: ConsumerMessage): Promise<void> {
    const requestCancel = new AbortController();
    let context = (
      owner instanceof MessageContext ? owner : MessageContext.fromContext(owner)
    ).withExternalCancellation(requestCancel.signal);
    const metadata = new Map(context.metadata());
    for (const [key, value] of entryMetadata(message.message)) metadata.set(key, value);
    context = applyDataSourceEndpointTracing(
      context.withMetadata(metadata),
      this.ep.runtimeEnvironment(),
      this.ep.id
    );
    // Preserve correlation from the caller; fence delivery-local result ownership separately.
    context = context.withStreamId(context.streamId() ?? newStreamId());
    let state: H;
    try {
      const begun = await this.handler.beginRequest(context, this.#sc);
      context = begun.context.withExternalCancellation(owner.signal());
      state = begun.state;
    } catch (error) {
      this.ep.onBeginRequestFailed(context, errorFromUnknown(error));
      requestCancel.abort();
      return;
    }
    const id = context.streamId() ?? newStreamId();
    context = context.withStreamId(id);
    settlementContexts.set(message, context);
    const request = new Request<H, T, R, E>(state);
    context = context.withLocalValue(requestIdentity, request.identity);
    const hasResult = this.stream().resultStream() !== undefined;
    const started = this.ep.onRequestStart(context);
    if (hasResult) {
      this.#pending.set(request.identity, request);
      this.ep.onPendingAdd(context, id);
    }
    let failure: Error | undefined;
    let waitCancelled = false;
    try {
      await this.handler.consumeMessage(context, this.#sc, state, message, request);
      if (hasResult && !request.completed) {
        try {
          await wait(request.completion, context);
        } catch (error) {
          failure = errorFromUnknown(error);
          waitCancelled = true;
        }
      } else if (!hasResult && context.cancelled()) failure = cancellation(context);
    } catch (error) {
      failure = errorFromUnknown(error);
    } finally {
      request.retired = true;
      // Callbacks already running own state until they return; Stop still has a deadline.
      await Promise.allSettled([...request.active]);
      if (waitCancelled && request.completed) failure = undefined;
      this.#pending.delete(request.identity);
      request.callbacks.clear();
      if (hasResult) this.ep.onPendingRemove(context, id);
      try {
        await this.handler.endRequest(context, this.#sc, failure, state);
      } catch (error) {
        failure ??= errorFromUnknown(error);
        await this.ep.report(context, failure);
      }
      this.ep.onRequestEnd(context, started, failure);
      requestCancel.abort(new Error("RedisStreams request finalized"));
    }
  }
  private async result(context: MessageContext, value: R): Promise<void> {
    const id = context.streamId();
    if (!id) {
      this.ep.onMissingStreamId(context);
      return;
    }
    const identity = context.localValue(requestIdentity);
    const request = identity ? this.#pending.get(identity) : undefined;
    if (
      !request ||
      request.retired ||
      request.completed ||
      context.localValue(requestIdentity) !== request.identity
    ) {
      this.ep.onLateResult(context, id);
      return;
    }
    if (request.active.size >= PENDING_MESSAGES)
      throw new Error("RedisStreams active result callback limit exceeded");
    const operation = (async () => {
      const messageId = this.handler.getMessageId(context, this.#sc, request.state, value);
      const callback = request.callbacks.get(messageId);
      if (!callback) {
        this.ep.onUnknownMessageId(context, id, messageId);
        return;
      }
      if (await callback(context, this.#sc, request.state, value))
        request.callbacks.delete(messageId);
    })();
    request.active.add(operation);
    try {
      await operation;
    } finally {
      request.active.delete(operation);
    }
  }
}

export function makeRedisStreamsEndpointConsumer<H, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  handler: EndpointHandler<H, T, R, E>
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const config = requireRedisStreamsEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const existing = environment.dataSourceById(config.idDataConnector);
  if (existing && !(existing instanceof RedisStreamsDataSource))
    throw new Error("connector already has a different source adapter");
  const source = existing ?? new RedisStreamsDataSource(config.idDataConnector, environment);
  if (!existing) environment.addDataSource(source);
  if (source.endpoint(config.id)) throw new Error("duplicate RedisStreams source endpoint");
  const ep = new SourceEndpoint(source, config.id);
  const consumer = new EndpointConsumer(ep, stream, handler);
  ep.addEndpointConsumer(consumer);
  source.addEndpoint(ep);
  return consumer;
}

export { DeletedPendingEntriesError, type Entry } from "../../internal/redis/client.js";
