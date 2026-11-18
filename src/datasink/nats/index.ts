import { PublicationMetrics } from "../../internal/broker-publication-metrics.js";
import { headers, type MsgHdrs, type NatsConnection } from "@nats-io/transport-node";
import type { JetStreamClient, PubAck } from "@nats-io/jetstream";
import {
  OutputDataSink,
  DataSinkEndpoint,
  MessageContextKey,
  requireNATSDataConnectorConfig,
  requireNATSEndpointConfig,
  validateNATSEndpoint,
  errorFromUnknown,
  err,
  type Context,
  type MessageContext,
  type RuntimeEnvironment,
  type Completion,
  type Consumer,
  type TypedSinkStream,
  type TypedSinkStreamWithResult,
  type Stream
} from "../../runtime/index.js";
import {
  cancellation,
  messageBytes,
  openConnection,
  resources,
  wait,
  PENDING_MESSAGES,
  PENDING_BYTES
} from "../../internal/nats/client.js";

/** Core flush is a protocol barrier, not subscriber processing or persistence. */
export interface Delivery {
  readonly ack: PubAck | undefined;
}
export type DeliveryCallback<R> = (
  delivery: Delivery | undefined,
  error: Error | undefined
) => R | undefined;
/** Typed native sink context; plain sinks expose errors without inventing result streams. */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export class StreamContext<T, R, E> {
  public readonly logger: ReturnType<RuntimeEnvironment["log"]>;
  public constructor(
    public readonly stream: Stream,
    private readonly output: (context: MessageContext, value: R) => Completion,
    private readonly errors: (context: MessageContext, value: E) => Completion
  ) {
    this.logger = stream.runtimeEnvironment().log();
  }
  public collect(context: MessageContext, value: R): Completion {
    return this.output(context, value);
  }
  public errorCollect(context: MessageContext, value: E): Completion {
    return this.errors(context, value);
  }
}
export interface EndpointHandler<H, T, R, E> {
  getStreamId(context: MessageContext, value: Readonly<T>): string;
  beginRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>
  ): { context: MessageContext; state: H } | Promise<{ context: MessageContext; state: H }>;
  consumeMessage(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    state: H,
    value: Readonly<T>,
    message: SinkMessage<R>
  ): Completion;
  endRequest(
    context: MessageContext,
    stream: StreamContext<T, R, E>,
    error: Error | undefined,
    state: H
  ): Completion;
}

class RequestScope {
  public readonly controller = new AbortController();
  #references = 1;
  public retain(): void {
    this.#references++;
  }
  public release(): void {
    if (--this.#references === 0) this.controller.abort(new Error("NATS request finalized"));
  }
}
const requestScope = new MessageContextKey<RequestScope | undefined>(undefined);

export class SinkMessage<R> {
  #active = true;
  public retire(): void {
    this.#active = false;
  }
  public data: Uint8Array = new Uint8Array();
  public header: MsgHdrs = headers();
  public constructor(
    private readonly ep: SinkEndpoint,
    private readonly output: (context: MessageContext, result: R) => Completion
  ) {}
  public subject(): string {
    return requireNATSEndpointConfig(this.ep.config()).subject;
  }
  /** Returns after bounded admission; callback follows native publication outcome. */
  public send(context: MessageContext, onDelivery: DeliveryCallback<R>): void {
    if (!this.#active) {
      this.ep.publication.observe(context, "error");
      throw new Error("NATS SinkMessage cannot be retained after ConsumeMessage");
    }
    this.ep.send(context, this.data, this.header, async (delivery, error) => {
      const result = onDelivery(delivery, error);
      if (result !== undefined) await this.output(context, result);
    });
  }
  /** Await the same native outcome; does not emit a graph result implicitly. */
  public sendSync(context: MessageContext): Promise<Delivery> {
    if (!this.#active) {
      this.ep.publication.observe(context, "error");
      return Promise.reject(new Error("NATS SinkMessage cannot be retained after ConsumeMessage"));
    }
    return this.ep.sendSync(context, this.data, this.header);
  }
  public out(context: MessageContext, result: R): Completion {
    return this.output(context, result);
  }
  public skip(context: MessageContext, result: R): Completion {
    return this.out(context, result);
  }
}

class SinkEndpoint extends DataSinkEndpoint {
  public readonly publication = new PublicationMetrics(
    this.runtimeEnvironment().metrics(),
    this.dataSink().config().type === 8 ? "jetstream" : "nats",
    this.dataSink().name,
    this.name
  );
  #connection: NatsConnection | undefined;
  #js: JetStreamClient | undefined;
  #controller = new AbortController();
  #owner: Context | undefined;
  #accepting = false;
  #started = false;
  #stopped = false;
  #connected = false;
  #tasks = new Set<Promise<void>>();
  #handlers = new Set<Promise<void>>();
  #bytes = 0;
  #timeout = 5000;

  public async start(parent: Context): Promise<void> {
    if (this.#started || this.#stopped) throw new Error("NATS sink already started or stopped");
    this.#started = true;
    const ep = requireNATSEndpointConfig(this.config());
    if (!ep.enabled) return;
    const dc = requireNATSDataConnectorConfig(this.dataSink().config());
    validateNATSEndpoint(ep, dc.type, true);
    this.#timeout = dc.operationTimeoutMs;
    const owner = parent.withExternalCancellation(this.#controller.signal);
    this.#owner = owner;
    const nc = await openConnection(dc, owner);
    this.#connection = nc;
    try {
      if (dc.type === 8)
        this.#js = (
          await wait(resources(nc, ep, dc.operationTimeoutMs, false), owner, dc.operationTimeoutMs)
        ).js;
      if (this.stopped() || owner.cancelled()) throw new Error("NATS sink stopped during startup");
      this.#connected = true;
      this.#accepting = true;
      void nc.closed().then((error) => {
        this.#connected = false;
        this.#accepting = false;
        if (error) this.report(owner, error);
      });
      void (async () => {
        for await (const status of nc.status()) {
          if (status.type === "disconnect" || status.type === "reconnecting")
            this.#connected = false;
          if (status.type === "reconnect") this.#connected = true;
          if (status.type === "error") this.report(owner, status.error);
        }
      })().catch((error: unknown) => {
        this.report(owner, errorFromUnknown(error));
      });
    } catch (error) {
      await nc.close();
      throw error;
    }
  }
  private stopped(): boolean {
    return this.#stopped;
  }
  public report(context: Context, error: Error): void {
    this.runtimeEnvironment().log().error(context, "NATS sink failure", err(error));
  }
  public runHandler(
    context: MessageContext,
    handler: (context: MessageContext) => Promise<void>
  ): Promise<void> {
    if (!this.#accepting || !this.#owner)
      return Promise.reject(new Error("NATS sink is not accepting requests"));
    if (this.#handlers.size >= PENDING_MESSAGES)
      return Promise.reject(new Error("NATS sink handler admission limit exceeded"));
    const scope = new RequestScope();
    const owner = context
      .withExternalCancellation(scope.controller.signal)
      .withExternalCancellation(this.#owner.signal())
      .withLocalValue(requestScope, scope);
    if (owner.cancelled()) {
      scope.release();
      return Promise.reject(cancellation(owner));
    }
    const task = Promise.resolve()
      .then(() => handler(owner))
      .finally(() => {
        scope.release();
      });
    this.#handlers.add(task);
    void task.then(
      () => this.#handlers.delete(task),
      () => this.#handlers.delete(task)
    );
    return task;
  }
  private reserve(
    context: MessageContext,
    data: Uint8Array,
    header: MsgHdrs
  ): { context: MessageContext; data: Uint8Array; header: MsgHdrs; bytes: number } {
    // Admitted handlers may publish while shutdown is draining; new handlers cannot enter.
    if (
      !this.#connection ||
      !this.#owner ||
      this.#controller.signal.aborted ||
      !this.#connected ||
      this.#connection.isClosed()
    )
      throw new Error("NATS sink connection unavailable");
    const owner = context;
    if (owner.cancelled() || this.#owner.cancelled()) throw cancellation(owner);
    const copy = headers();
    for (const [key, values] of header) for (const value of values) copy.append(key, value);
    for (const [key, value] of context.transportMetadata()) {
      for (const existing of copy.keys()) if (existing.toLowerCase() === key) copy.delete(existing);
      copy.set(key, value);
    }
    const bytes = messageBytes(data, this.subject(), copy);
    if (this.#tasks.size >= PENDING_MESSAGES || this.#bytes + bytes > PENDING_BYTES)
      throw new Error("NATS sink admission limit exceeded: 256 publications / 8 MiB");
    this.#bytes += bytes;
    return { context: owner, data: data.slice(), header: copy, bytes };
  }
  private subject(): string {
    return requireNATSEndpointConfig(this.config()).subject;
  }
  private publish(context: MessageContext, data: Uint8Array, header: MsgHdrs): Promise<Delivery> {
    const nc = this.#connection;
    if (!nc) return Promise.reject(new Error("NATS connection unavailable"));
    const config = requireNATSEndpointConfig(this.config());
    if (context.cancelled()) return Promise.reject(cancellation(context));
    // Both paths use the official SDK asynchronously. Cancellation is an ambiguous
    // outcome after transmission, never a claim that a publication was withdrawn.
    const operation: Promise<Delivery> = this.#js
      ? this.#js
          .publish(config.subject, data, {
            headers: header,
            timeout: this.#timeout,
            expect: { ...(config.streamName ? { streamName: config.streamName } : {}) },
            retries: 1 // SDK counts total attempts; do not retry ambiguous publications.
          })
          .then((ack) => ({ ack }))
      : (() => {
          nc.publish(config.subject, data, { headers: header });
          return nc.flush().then(() => ({ ack: undefined }));
        })();
    return wait(operation, context, this.#timeout);
  }
  public send(
    context: MessageContext,
    data: Uint8Array,
    header: MsgHdrs,
    callback: (delivery: Delivery | undefined, error: Error | undefined) => Completion
  ): void {
    try {
      let reserved;
      try {
        reserved = this.reserve(context, data, header);
      } catch (error) {
        throw errorFromUnknown(error);
      }
      const { context: owner, data: payload, header: copied, bytes } = reserved;
      const scope = context.localValue(requestScope);
      scope?.retain();
      // Deferring execution one microtask makes reservation visible before callbacks
      // or reentrant sends run; no unbounded detached callback queue.
      const task = Promise.resolve().then(async () => {
        let delivery: Delivery | undefined;
        let failure: Error | undefined;
        try {
          delivery = await this.publish(owner, payload, copied);
        } catch (error) {
          failure = errorFromUnknown(error);
        }
        this.publication.observe(
          context,
          failure ? "error" : delivery?.ack ? "acknowledged" : "flushed"
        );
        try {
          await callback(delivery, failure);
        } finally {
          // A timed-out/cancelled SDK publication can still be in reconnect buffers.
          // Keep its count/byte reservation until an actual flush or connection close;
          // otherwise repeated timeouts could grow native buffers without bound.
          if (failure && this.#connection && this.#owner && !this.#connection.isClosed())
            await wait(this.#connection.flush(), this.#owner);
        }
      });
      this.#tasks.add(task);
      void task.then(
        () => {
          this.#tasks.delete(task);
          this.#bytes -= bytes;
          scope?.release();
        },
        (error: unknown) => {
          this.#tasks.delete(task);
          this.#bytes -= bytes;
          scope?.release();
          this.report(context, errorFromUnknown(error));
        }
      );
    } catch (error) {
      this.publication.observe(context, "error");
      throw error;
    }
  }
  public sendSync(context: MessageContext, data: Uint8Array, header: MsgHdrs): Promise<Delivery> {
    return new Promise((resolve, reject) => {
      try {
        this.send(context, data, header, (delivery, error) => {
          if (error) reject(error);
          else if (delivery) resolve(delivery);
          else reject(new Error("NATS publication had no outcome"));
        });
      } catch (error) {
        reject(errorFromUnknown(error));
      }
    });
  }
  public stopAdmission(): void {
    this.#stopped = true;
    this.#accepting = false;
  }
  public async stop(context: Context): Promise<void> {
    this.stopAdmission();
    try {
      await wait(
        (async () => {
          while (this.#handlers.size || this.#tasks.size)
            await Promise.allSettled([...this.#handlers, ...this.#tasks]);
        })(),
        context
      );
    } catch (error) {
      this.report(context, errorFromUnknown(error));
      throw error;
    } finally {
      this.#controller.abort(new Error("NATS sink stopped"));
      await this.#connection?.close();
    }
  }
}

export class NATSDataSink extends OutputDataSink {
  public constructor(id: number, environment: RuntimeEnvironment) {
    super(id, environment);
  }
  public async start(context: Context): Promise<void> {
    try {
      for (const endpoint of this.endpoints()) await (endpoint as SinkEndpoint).start(context);
    } catch (error) {
      await this.stop(context.bounded(1000)).catch(() => undefined);
      throw error;
    }
  }
  public async stop(context: Context): Promise<void> {
    for (const endpoint of this.endpoints()) (endpoint as SinkEndpoint).stopAdmission();
    const results = await Promise.allSettled(
      this.endpoints().map((endpoint) => (endpoint as SinkEndpoint).stop(context))
    );
    const failure = results.find((result) => result.status === "rejected");
    if (failure?.status === "rejected") throw errorFromUnknown(failure.reason);
  }
}

type Sink<T, E> = TypedSinkStream<T, E> & Stream;
function makeConsumer<H, T, R, E>(
  stream: Sink<T, E>,
  handler: EndpointHandler<H, T, R, E>,
  collect: (context: MessageContext, value: R) => Completion
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  const config = requireNATSEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const existing = environment.dataSinkById(config.idDataConnector);
  if (existing && !(existing instanceof NATSDataSink))
    throw new Error("connector already has a different sink adapter");
  const sink = existing ?? new NATSDataSink(config.idDataConnector, environment);
  if (!existing) environment.addDataSink(sink);
  const oldEndpoint = sink.endpoint(config.id);
  if (oldEndpoint && !(oldEndpoint instanceof SinkEndpoint))
    throw new Error("endpoint already uses a different sink adapter");
  const ep = oldEndpoint ?? new SinkEndpoint(sink, config.id);
  if (!oldEndpoint) sink.addEndpoint(ep);
  const sc = new StreamContext<T, R, E>(stream, collect, (context, value: E) =>
    stream.errorStream().consume(context, value)
  );
  const consumer: Consumer<T> = {
    consume: (context, value) =>
      ep.runHandler(context, async (owner) => {
        const request = owner.withStreamId(handler.getStreamId(owner, value));
        let begun;
        try {
          begun = await handler.beginRequest(request, sc);
        } catch (error) {
          ep.onBeginRequestFailed(request, errorFromUnknown(error));
          return;
        }
        const handlerContext = begun.context.withExternalCancellation(owner.signal());
        const start = ep.onRequestStart(handlerContext);
        let failure: Error | undefined;
        const message = new SinkMessage(ep, collect);
        try {
          await handler.consumeMessage(handlerContext, sc, begun.state, value, message);
        } catch (error) {
          failure = errorFromUnknown(error);
        } finally {
          message.retire();
        }
        try {
          await handler.endRequest(handlerContext, sc, failure, begun.state);
        } catch (error) {
          failure ??= errorFromUnknown(error);
          ep.report(handlerContext, failure);
        } finally {
          ep.onRequestEnd(handlerContext, start, failure);
        }
      })
  };
  stream.setSinkConsumer(consumer);
  ep.addEndpointConsumer({ endpoint: () => ep });
  return consumer;
}

export function makeNATSEndpointConsumer<H, T, E>(
  stream: TypedSinkStream<T, E>,
  handler: EndpointHandler<H, T, never, E>
): Consumer<T> {
  return makeConsumer(stream as Sink<T, E>, handler, () => {
    throw new Error("NATS sink has no result stream");
  });
}
export function makeNATSEndpointConsumerWithResult<H, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  handler: EndpointHandler<H, T, R, E>
): Consumer<T> {
  return makeConsumer(stream as Sink<T, E>, handler, (context, value) =>
    stream.consumeResult(context, value)
  );
}
