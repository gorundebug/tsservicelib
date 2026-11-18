import { PublicationMetrics } from "../../internal/broker-publication-metrics.js";
import {
  OutputDataSink,
  DataSinkEndpoint,
  MessageContextKey,
  requirePulsarDataConnectorConfig,
  requirePulsarEndpointConfig,
  validatePulsarEndpoint,
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
  AdmissionRejected,
  cancellation,
  wait,
  Session,
  snapshot,
  BYTE_LIMIT,
  type Delivery,
  type ProducerMessage
} from "../../internal/pulsar/client.js";
export type { Delivery, ProducerMessage } from "../../internal/pulsar/client.js";

export type DeliveryCallback<R> = (
  delivery: Delivery | undefined,
  error: Error | undefined
) => R | undefined | Promise<R | undefined>;
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
    if (--this.#references === 0) this.controller.abort(new Error("Pulsar request finalized"));
  }
}
const requestScope = new MessageContextKey<RequestScope | undefined>(undefined);

export class SinkMessage<R> {
  #active = true;
  public retire(): void {
    this.#active = false;
  }
  public message: ProducerMessage = { data: Buffer.alloc(0) };
  public constructor(
    private readonly ep: SinkEndpoint,
    private readonly output: (context: MessageContext, result: R) => Completion
  ) {}
  /** Returns after bounded admission; callback follows native publication outcome. */
  public send(context: MessageContext, onDelivery: DeliveryCallback<R>): void {
    if (!this.#active) {
      this.ep.publication.observe(context, "not-sent");
      throw new AdmissionRejected("Pulsar SinkMessage cannot be retained after ConsumeMessage");
    }
    this.ep.send(context, this.message, async (delivery, error) => {
      const result = await onDelivery(delivery, error);
      if (result !== undefined) await this.output(context, result);
    });
  }
  /** Await the same native outcome; does not emit a graph result implicitly. */
  public sendSync(context: MessageContext): Promise<Delivery> {
    if (!this.#active) {
      this.ep.publication.observe(context, "not-sent");
      return Promise.reject(
        new AdmissionRejected("Pulsar SinkMessage cannot be retained after ConsumeMessage")
      );
    }
    return this.ep.sendSync(context, this.message);
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
    "pulsar",
    this.dataSink().name,
    this.name
  );
  #session: Session | undefined;
  #controller = new AbortController();
  #owner: Context | undefined;
  #accepting = false;
  #started = false;
  #stopped = false;
  #tasks = new Set<Promise<void>>();
  #handlers = new Set<Promise<void>>();
  #bytes = 0;
  public async start(parent: Context): Promise<void> {
    if (this.#started || this.#stopped)
      throw new AdmissionRejected("Pulsar sink already started/stopped");
    this.#started = true;
    const ep = requirePulsarEndpointConfig(this.config());
    if (!ep.enabled) return;
    const dc = requirePulsarDataConnectorConfig(this.dataSink().config());
    validatePulsarEndpoint(ep, true);
    const owner = parent.withExternalCancellation(this.#controller.signal);
    this.#owner = owner;
    this.#session = new Session(dc, ep, false, (error: unknown) => {
      this.report(owner, errorFromUnknown(error));
    });
    await this.#session.start(owner);
    this.#accepting = true;
  }
  public report(context: Context, error: Error): void {
    this.runtimeEnvironment().log().error(context, "Pulsar sink failure", err(error));
  }
  public runHandler(
    context: MessageContext,
    handler: (context: MessageContext) => Promise<void>
  ): Promise<void> {
    if (!this.#accepting || !this.#owner)
      return Promise.reject(new Error("Pulsar sink is not accepting requests"));
    if (this.#handlers.size >= (this.#session?.limit ?? 256))
      return Promise.reject(new Error("Pulsar sink handler admission limit exceeded"));
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

  public send(
    context: MessageContext,
    fields: ProducerMessage,
    callback: (delivery: Delivery | undefined, error: Error | undefined) => Completion
  ): void {
    try {
      const session = this.#session;
      if (!this.#accepting || !session?.ready || this.#controller.signal.aborted)
        throw new AdmissionRejected("Pulsar connection unavailable");
      if (context.cancelled())
        throw new AdmissionRejected("Pulsar publication cancelled before admission", {
          cause: cancellation(context)
        });
      const copied = snapshot(fields, context);
      if (this.#tasks.size >= session.limit || this.#bytes + copied.bytes > BYTE_LIMIT)
        throw new AdmissionRejected("Pulsar publication count/byte admission limit exceeded");
      this.#bytes += copied.bytes;
      const scope = context.localValue(requestScope);
      scope?.retain();
      const task = Promise.resolve().then(async () => {
        let delivery: Delivery;
        try {
          delivery = await session.publish(copied.message, context);
        } catch {
          delivery = { outcome: "Unknown", error: new Error("Pulsar publication failed") };
        }
        this.publication.observe(context, delivery.outcome);
        await callback(delivery, delivery.error);
      });
      this.#tasks.add(task);
      void task.then(
        () => {
          finish();
        },
        (error: unknown) => {
          finish();
          this.report(context, errorFromUnknown(error));
        }
      );
      const finish = (): void => {
        this.#tasks.delete(task);
        this.#bytes -= copied.bytes;
        scope?.release();
      };
    } catch (error) {
      this.publication.observe(context, "not-sent");
      throw error;
    }
  }
  public sendSync(context: MessageContext, fields: ProducerMessage): Promise<Delivery> {
    return new Promise((resolve, reject) => {
      try {
        this.send(context, fields, (delivery) => {
          resolve(delivery ?? { outcome: "Unknown" });
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
    } finally {
      this.#controller.abort(new Error("Pulsar sink stopped"));
      await this.#session?.close(context);
    }
  }
}

export class PulsarDataSink extends OutputDataSink {
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
  const config = requirePulsarEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const existing = environment.dataSinkById(config.idDataConnector);
  if (existing && !(existing instanceof PulsarDataSink))
    throw new Error("connector already has a different sink adapter");
  const sink = existing ?? new PulsarDataSink(config.idDataConnector, environment);
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
        const handlerContext = begun.context
          .withExternalCancellation(owner.signal())
          .withLocalValue(requestScope, owner.localValue(requestScope));
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

export function makePulsarEndpointConsumer<H, T, E>(
  stream: TypedSinkStream<T, E>,
  handler: EndpointHandler<H, T, never, E>
): Consumer<T> {
  return makeConsumer(stream as Sink<T, E>, handler, () => {
    throw new AdmissionRejected("Pulsar sink has no result stream");
  });
}
export function makePulsarEndpointConsumerWithResult<H, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>,
  handler: EndpointHandler<H, T, R, E>
): Consumer<T> {
  return makeConsumer(stream as Sink<T, E>, handler, (context, value) =>
    stream.consumeResult(context, value)
  );
}
