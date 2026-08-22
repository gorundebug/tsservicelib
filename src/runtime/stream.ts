import type { MessageContext } from "./context.js";
import type { StreamConfig } from "./config/index.js";
import { transformationName } from "./config/index.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import { stringAttribute, type StartedSpan, type Tracer } from "./environment/index.js";
import type { StreamSerde } from "./serde/index.js";

export type Completion = void | Promise<void>;

export interface Stream {
  readonly id: number;
  readonly name: string;
  readonly transformationName: string;
  runtimeEnvironment(): RuntimeEnvironment;
  config(): StreamConfig;
}

export interface Consumer<T> {
  consume(context: MessageContext, value: T): Completion;
}

export interface Caller<T> extends Consumer<T> {
  isAsync(): boolean;
}

export interface TypedStreamConsumer<T> extends Stream, Consumer<T> {}

export interface TypedConsumedStream<T> extends TypedStream<T>, TypedStreamConsumer<T> {}

export interface TypedStream<T> extends Stream {
  serde(): StreamSerde<T>;
  typeName(): string;
  consumer(): TypedStreamConsumer<T> | undefined;
  consumers(): readonly Stream[];
  setConsumer(consumer: TypedStreamConsumer<T>): void;
}

export interface CallerFactory {
  create<T>(source: Stream, consumer: TypedStreamConsumer<T>): Caller<T>;
}

/**
 * Direct delivery preserves FunctionCall semantics. The async bit is graph
 * metadata and never turns this call into detached work.
 */
export class FunctionCaller<T> implements Caller<T> {
  readonly #consumer: Consumer<T>;
  readonly #async: boolean;

  public constructor(consumer: Consumer<T>, async = false) {
    this.#consumer = consumer;
    this.#async = async;
  }

  public isAsync(): boolean {
    return this.#async;
  }

  public consume(context: MessageContext, value: T): Completion {
    return this.#consumer.consume(context, value);
  }
}

/** Stores only immutable graph identity; reloadable config is resolved elsewhere by ID. */
export class ServiceStream implements Stream {
  readonly #id: number;
  readonly #environment: RuntimeEnvironment;
  readonly #name: string;
  readonly #tracer: Tracer | undefined;
  public readonly transformationName: string;

  public constructor(config: StreamConfig, environment: RuntimeEnvironment) {
    this.#id = config.id;
    this.#environment = environment;
    this.#tracer = environment.tracing()?.tracer(environment.serviceConfig().name);
    this.#name = config.name;
    this.transformationName = transformationName(config.type);
  }

  public get id(): number {
    return this.#id;
  }

  public get name(): string {
    return this.#name;
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    return this.#environment;
  }

  public config(): StreamConfig {
    const config = this.#environment.runtimeConfig().streamById(this.#id);
    if (config === undefined) {
      throw new Error(`stream config ${String(this.#id)} not found`);
    }
    return config;
  }

  protected tracingEnabled(context: MessageContext): boolean {
    return this.#tracer !== undefined && context.samplingEnabled();
  }

  protected startSpan(context: MessageContext, operation: string): StartedSpan | undefined {
    if (!this.tracingEnabled(context)) {
      return undefined;
    }
    return this.#tracer?.start(context, operation, [stringAttribute("stream", this.name)]);
  }

  protected traceCompletion(
    context: MessageContext,
    operation: string,
    consume: (spanContext: MessageContext) => Completion
  ): Completion {
    const started = this.startSpan(context, operation);
    if (started === undefined) {
      return consume(context);
    }
    let completion: Completion;
    try {
      completion = consume(started.context);
    } catch (error: unknown) {
      started.span.end();
      throw error;
    }
    if (completion === undefined) {
      started.span.end();
      return;
    }
    return completion.finally(() => {
      started.span.end();
    });
  }
}
