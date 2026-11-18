import { SourceMetrics } from "../../internal/broker-source-metrics.js";
import type { Msg, NatsConnection, Subscription } from "@nats-io/transport-node";
import type { ConsumerMessages, JsMsg } from "@nats-io/jetstream";
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
  requireNATSDataConnectorConfig,
  requireNATSEndpointConfig,
  validateNATSEndpoint,
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
  messageBytes,
  openConnection,
  resources,
  wait,
  PENDING_BYTES,
  PENDING_MESSAGES
} from "../../internal/nats/client.js";

/** Real SDK deliveries. Core has no acknowledgement operations. */
export type ConsumerMessage =
  | { readonly message: Msg; readonly jetstream: undefined }
  | { readonly message: JsMsg; readonly jetstream: JsMsg };
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
    if (this.retired) throw new Error("NATS request already retired");
    if (this.callbacks.size >= PENDING_MESSAGES)
      throw new Error("NATS result callback limit exceeded");
    this.callbacks.set(id, callback);
  }
}

class SourceEndpoint extends DataSourceEndpoint {
  public readonly observations = new SourceMetrics(this.runtimeEnvironment().metrics(), this.dataSource().config().type === 8 ? "jetstream" : "nats", this.dataSource().name, this.name);
  public handle!: (context: Context, message: ConsumerMessage) => Promise<void>;
  public report!: (context: Context, error: Error) => Promise<void>;
  #controller = new AbortController();
  #started = false;
  #stopping = false;
  #connection: NatsConnection | undefined;
  #subscription: Subscription | undefined;
  #messages: ConsumerMessages | undefined;
  #run: Promise<void> = Promise.resolve();
  #wake: (() => void) | undefined;
  #queue: Msg[] = [];
  #bytes = 0;
  #failure: Error | undefined;
  #owner: Context | undefined;
  #reporting: Promise<void> = Promise.resolve();

  public async start(parent: Context): Promise<void> {
    if (this.#started || this.#stopping) throw new Error("NATS source already started or stopped");
    this.#started = true;
    const ep = requireNATSEndpointConfig(this.config());
    if (!ep.enabled) return;
    const dc = requireNATSDataConnectorConfig(this.dataSource().config());
    validateNATSEndpoint(ep, dc.type, false);
    const owner = parent.withExternalCancellation(this.#controller.signal);
    this.#owner = owner;
    const nc = await openConnection(dc, owner);
    this.#connection = nc;
    if (this.stopping()) {
      await nc.close();
      throw new Error("NATS source stopped during startup");
    }
    try {
      if (dc.type === 8) {
        const { consumer } = await wait(
          resources(nc, ep, dc.operationTimeoutMs, true),
          owner,
          dc.operationTimeoutMs
        );
        // Native pull iterator bounds prefetch independently of graph execution.
        if ((nc.info?.max_payload ?? Infinity) > PENDING_BYTES)
          throw new Error("NATS.js bounded pull profile requires server max_payload <= 8 MiB");
        if (!consumer) throw new Error("JetStream consumer unavailable");
        this.#messages = await consumer.consume({
          max_messages: 1,
          abort_on_missing_resource: true
        });
        const messages = this.#messages;
        this.#run = (async () => {
          for await (const message of messages) {
            if (this.#stopping) break;
            if (messageBytes(message.data, message.subject, message.headers) > PENDING_BYTES)
              throw new Error("JetStream message exceeds 8 MiB admission bound");
            await this.handle(owner, { message, jetstream: message });
          }
        })();
      } else {
        this.#subscription = nc.subscribe(ep.subject, {
          ...(ep.queueGroup ? { queue: ep.queueGroup } : {}),
          callback: (error, message) => {
            if (this.#stopping) return;
            if (error) {
              this.fail(error);
              return;
            }
            const bytes = messageBytes(message.data, message.subject, message.headers);
            if (this.#queue.length >= PENDING_MESSAGES || this.#bytes + bytes > PENDING_BYTES) {
              this.fail(
                new Error(
                  "NATS source overflow: 256 pending messages / 8 MiB limit; subscription stopped"
                )
              );
              return;
            }
            this.#queue.push(message);
            this.#bytes += bytes;
            this.#wake?.();
          }
        });
        await wait(nc.flush(), owner, dc.operationTimeoutMs);
        this.#run = (async () => {
          while (!owner.cancelled()) {
            const message = this.#queue.shift();
            if (message) {
              this.#bytes -= messageBytes(message.data, message.subject, message.headers);
              await this.handle(owner, { message, jetstream: undefined });
            } else if (this.#stopping || this.#failure) break;
            else
              await new Promise<void>((resolve) => {
                this.#wake = resolve;
              });
          }
        })();
      }
      this.#run = this.#run.catch(async (error: unknown) => {
        await this.report(owner, errorFromUnknown(error));
      });
      void nc.closed().then((error) => {
        if (error) this.fail(error);
        else this.stopInput();
        this.#wake?.();
      });
      // Consume status notifications so native permission/connection errors stay observable.
      void (async () => {
        for await (const status of nc.status())
          if (status.type === "error") await this.report(owner, status.error);
      })().catch((error: unknown) => {
        this.runtimeEnvironment()
          .log()
          .error(owner, "NATS source status failure", err(errorFromUnknown(error)));
      });
      owner.signal().addEventListener(
        "abort",
        () => {
          this.stopInput();
        },
        { once: true }
      );
      if (owner.cancelled()) this.stopInput();
    } catch (error) {
      await nc.close();
      throw error;
    }
  }

  private stopping(): boolean {
    return this.#stopping;
  }
  private fail(error: Error): void {
    if (!this.#failure && this.#owner) this.#reporting = this.report(this.#owner, error);
    this.#failure ??= error;
    this.stopInput();
  }
  public stopInput(): void {
    this.#stopping = true;
    this.#subscription?.unsubscribe();
    void this.#messages?.close();
    this.#wake?.();
  }
  public async stop(context: Context): Promise<void> {
    this.stopInput();
    try {
      await wait(Promise.all([this.#run, this.#reporting]), context);
    } catch (error) {
      this.runtimeEnvironment()
        .log()
        .error(context, "NATS source drain incomplete", err(errorFromUnknown(error)));
      throw error;
    } finally {
      this.#controller.abort(new Error("NATS source stopped"));
      this.#queue = [];
      this.#bytes = 0;
      await this.#connection?.close();
    }
  }
}

export class NATSDataSource extends InputDataSource {
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
      if (ep.observations.enabled && message.jetstream && message.jetstream.info.deliveryCount > 1) ep.observations.observe(context, "redelivered");
      return this.handle(context, message);
    };
    ep.report = async (context, error) => {
      try {
        await handler.onError(MessageContext.fromContext(context), this.#sc, error);
      } catch (failure) {
        ep.runtimeEnvironment()
          .log()
          .error(context, "NATS onError failed", err(errorFromUnknown(failure)));
      }
    };
  }
  private async handle(owner: Context, message: ConsumerMessage): Promise<void> {
    const requestCancel = new AbortController();
    let context = (
      owner instanceof MessageContext ? owner : MessageContext.fromContext(owner)
    ).withExternalCancellation(requestCancel.signal);
    const metadata = new Map(context.metadata());
    if (message.message.headers)
      for (const [key, values] of message.message.headers) {
        const value = values[0];
        if (value !== undefined) metadata.set(key.toLowerCase(), value);
      }
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
    const request = new Request<H, T, R, E>(state);
    context = context.withLocalValue(requestIdentity, request.identity);
    const hasResult = this.stream().resultStream() !== undefined;
    const started = this.ep.onRequestStart(context);
    if (hasResult) {
      this.#pending.set(id, request);
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
      this.#pending.delete(id);
      request.callbacks.clear();
      if (hasResult) this.ep.onPendingRemove(context, id);
      try {
        await this.handler.endRequest(context, this.#sc, failure, state);
      } catch (error) {
        failure ??= errorFromUnknown(error);
        await this.ep.report(context, failure);
      }
      this.ep.onRequestEnd(context, started, failure);
      requestCancel.abort(new Error("NATS request finalized"));
    }
  }
  private async result(context: MessageContext, value: R): Promise<void> {
    const id = context.streamId();
    if (!id) {
      this.ep.onMissingStreamId(context);
      return;
    }
    const request = this.#pending.get(id);
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
      throw new Error("NATS active result callback limit exceeded");
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

export function makeNATSEndpointConsumer<H, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  handler: EndpointHandler<H, T, R, E>
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const config = requireNATSEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const existing = environment.dataSourceById(config.idDataConnector);
  if (existing && !(existing instanceof NATSDataSource))
    throw new Error("connector already has a different source adapter");
  const source = existing ?? new NATSDataSource(config.idDataConnector, environment);
  if (!existing) environment.addDataSource(source);
  if (source.endpoint(config.id)) throw new Error("duplicate NATS source endpoint");
  const ep = new SourceEndpoint(source, config.id);
  const consumer = new EndpointConsumer(ep, stream, handler);
  ep.addEndpointConsumer(consumer);
  source.addEndpoint(ep);
  return consumer;
}
