import { ConsumedStream } from "../runtime/consumed-stream.js";
import type { SubStreamConfig } from "../runtime/config/types.js";
import { type MessageContext, MessageContextKey } from "../runtime/context.js";
import type { RuntimeEnvironment } from "../runtime/environment/runtime-environment.js";
import { errorFromUnknown } from "../runtime/errors.js";
import type { StreamSerde } from "../runtime/serde/serde.js";
import type {
  Completion,
  SubStream as CallableSubStream,
  SubStreamCollector,
  TypedStream,
  TypedStreamConsumer
} from "../runtime/stream.js";
import { StreamLink } from "./stream-link.js";

class SubStreamCall<R> {
  readonly #completion = Promise.withResolvers<undefined>();
  readonly #signal: AbortSignal;
  readonly #abort: () => void;
  #context: MessageContext | undefined;
  #collector: SubStreamCollector<R> | undefined;
  #active: Promise<void> | undefined;
  #closed = false;
  #failure: Error | undefined;

  public constructor(context: MessageContext, collector: SubStreamCollector<R>) {
    this.#context = context;
    this.#collector = collector;
    this.#signal = context.signal();
    this.#abort = (): void => {
      this.finish(errorFromUnknown(this.#signal.reason ?? new Error("substream cancelled")));
    };
    this.#signal.addEventListener("abort", this.#abort, { once: true });
    if (context.cancelled()) this.#abort();
  }

  public completion(): Promise<void> {
    return this.#completion.promise;
  }

  public throwIfFailed(): void {
    if (this.#failure !== undefined) throw this.#failure;
  }

  public deliver(value: R): Completion {
    if (this.#closed) return;
    if (this.#active !== undefined) {
      return this.#active.then(() => this.deliver(value));
    }
    const context = this.#context;
    const collector = this.#collector;
    if (context === undefined || collector === undefined) return;
    if (context.cancelled()) {
      this.#abort();
      return;
    }
    try {
      const complete = collector.out(context, value);
      if (typeof complete === "boolean") {
        if (complete) this.finish();
        return;
      }
      this.#active = complete
        .then(
          (done) => {
            if (done) this.finish();
          },
          (error: unknown) => {
            this.finish(errorFromUnknown(error));
            throw error;
          }
        )
        .finally(() => {
          this.#active = undefined;
        });
      return this.#active;
    } catch (error: unknown) {
      this.finish(errorFromUnknown(error));
      throw error;
    }
  }

  private finish(error?: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#failure = error;
    this.#collector = undefined;
    this.#context = undefined;
    this.#signal.removeEventListener("abort", this.#abort);
    this.#completion.resolve(undefined);
  }

  public async close(): Promise<void> {
    this.finish();
    // Cancellation stops new callbacks, but does not return while one is active.
    try {
      await this.#active;
    } catch {
      // The original body/collector error is reported by consume or its caller.
    }
  }
}

class ResultLink<T, R> extends StreamLink implements TypedStreamConsumer<R> {
  public constructor(
    entry: SubStream<T, R>,
    private readonly key: MessageContextKey<SubStreamCall<R> | undefined>
  ) {
    super(entry);
  }

  public consume(context: MessageContext, value: R): Completion {
    return context.localValue(this.key)?.deliver(value);
  }
}

/** A service-local callable entry; source is the ordinary result-producing stream. */
export class SubStream<T, R> extends ConsumedStream<T> implements CallableSubStream<T, R> {
  readonly #call = new MessageContextKey<SubStreamCall<R> | undefined>(undefined);
  #source: TypedStream<R> | undefined;

  public constructor(
    config: SubStreamConfig,
    environment: RuntimeEnvironment,
    serde: StreamSerde<T>
  ) {
    super(config, environment, serde);
    if (config.idService !== environment.serviceConfig().id) {
      throw new Error("SubStream must belong to its execution service");
    }
    environment.registerStream(this);
    environment.registerRuntimeBuildable(this);
  }

  public override setConsumer(consumer: TypedStreamConsumer<T>): void {
    if (consumer.config().idService !== this.config().idService) {
      throw new Error("SubStream body must belong to the same service");
    }
    super.setConsumer(consumer);
  }

  public setSource(source: TypedStream<R>): void {
    if (source.id === this.id || source.config().idService !== this.config().idService) {
      throw new Error("SubStream source must be a different stream in the same service");
    }
    if (this.#source !== undefined) {
      if (source === this.#source) return;
      throw new Error("SubStream source is already configured");
    }
    source.setConsumer(new ResultLink(this, this.#call));
    this.#source = source;
  }

  public build(): void {
    if (this.consumer() === undefined || this.#source === undefined) {
      throw new Error(`SubStream ${this.name} requires a body and a result source`);
    }
  }

  public async consume(
    context: MessageContext,
    value: T,
    collector: SubStreamCollector<R>
  ): Promise<void> {
    this.build();
    const call = new SubStreamCall(context, collector);
    try {
      call.throwIfFailed();
      const invocation = context.withLocalValue(this.#call, call);
      if (!this.tracingEnabled(invocation)) {
        await this.dispatch(invocation, value, call);
      } else {
        await this.traceCompletion(invocation, "stream.substream", (spanContext) =>
          this.dispatch(spanContext, value, call)
        );
      }
    } finally {
      await call.close();
    }
  }

  private async dispatch(context: MessageContext, value: T, call: SubStreamCall<R>): Promise<void> {
    await this.emit(context, value);
    const environment = this.runtimeEnvironment();
    if (environment.waitSubStreamResult === undefined) {
      await call.completion();
    } else {
      await environment.waitSubStreamResult(call.completion());
    }
    call.throwIfFailed();
  }
}

export function makeSubStream<T, R>(
  config: SubStreamConfig,
  environment: RuntimeEnvironment
): SubStream<T, R> {
  return new SubStream(config, environment, environment.serdeByName<T>(config.valueType));
}
