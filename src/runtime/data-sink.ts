import { performance } from "node:perf_hooks";

import type { Context, MessageContext } from "./context.js";
import type { Collector } from "./collector.js";
import { RuntimeDataConnector, type DataConnector, type Endpoint } from "./data-connector.js";
import {
  DataConnectorType,
  type DataConnectorConfig,
  type EndpointConfig
} from "./config/index.js";
import {
  err,
  str,
  type Float64Histogram,
  type Int64Counter,
  type Int64Gauge,
  type Logger,
  type RuntimeEnvironment
} from "./environment/index.js";
import type { Lifecycle } from "./lifecycle.js";
import type { StreamSerde } from "./serde/index.js";
import type {
  Completion,
  Consumer,
  TypedConsumedStream,
  TypedStream,
  TypedStreamConsumer
} from "./stream.js";

/** Runtime boundary implemented structurally by the Sink operator. */
export interface TypedSinkStream<T, E> extends TypedStreamConsumer<T> {
  endpointId(): number;
  errorStream(): TypedConsumedStream<E>;
  setSinkConsumer(consumer: Consumer<T>): void;
  inputSerde(): StreamSerde<T>;
}

/** Runtime boundary implemented structurally by the result-producing Sink operator. */
export interface TypedSinkStreamWithResult<T, R, E> extends TypedStream<R>, TypedStreamConsumer<T> {
  endpointId(): number;
  errorStream(): TypedConsumedStream<E>;
  setSinkConsumer(consumer: Consumer<T>): void;
  consumeResult(context: MessageContext, value: R): Completion;
  inputSerde(): StreamSerde<T>;
}

/** Common typed collector context passed to datasink endpoint handlers. */
// T is intentionally retained in the public signature to match the canonical
// SinkStreamContext<T, R, E>; the context only emits R and E by design.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export class SinkStreamContext<T, R, E> {
  public readonly stream: TypedStream<R>;
  public readonly logger: Logger;
  readonly #collector: Collector<R>;
  readonly #errorCollector: Collector<E>;

  public constructor(
    stream: TypedStream<R>,
    logger: Logger,
    collector: Collector<R>,
    errorCollector: Collector<E>
  ) {
    this.stream = stream;
    this.logger = logger;
    this.#collector = collector;
    this.#errorCollector = errorCollector;
  }

  public collect(context: MessageContext, value: R): Completion {
    return this.#collector.out(context, value);
  }

  public errorCollect(context: MessageContext, value: E): Completion {
    return this.#errorCollector.out(context, value);
  }
}

export function makeSinkStreamContext<T, R, E>(
  stream: TypedStream<R>,
  collector: Collector<R>,
  errorCollector: Collector<E>
): SinkStreamContext<T, R, E> {
  return new SinkStreamContext(
    stream,
    stream.runtimeEnvironment().log(),
    collector,
    errorCollector
  );
}

export interface DataSink extends DataConnector, Lifecycle {
  config(): DataConnectorConfig;
  runtimeEnvironment(): RuntimeEnvironment;
  addEndpoint(endpoint: SinkEndpoint): void;
  endpoint(id: number): SinkEndpoint | undefined;
  endpoints(): readonly SinkEndpoint[];
}

export interface SinkEndpoint extends Endpoint {
  dataSink(): DataSink;
  addEndpointConsumer(consumer: OutputEndpointConsumer): void;
  endpointConsumers(): readonly OutputEndpointConsumer[];
  onBeginRequestFailed(context: MessageContext, error: Error): void;
  onLateResult(context: MessageContext, streamId: string): void;
  onRequestStart(context: MessageContext): number | undefined;
  onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void;
}

export interface OutputEndpointConsumer {
  endpoint(): SinkEndpoint;
}

export abstract class OutputDataSink extends RuntimeDataConnector implements DataSink {
  readonly #endpoints = new Map<number, SinkEndpoint>();

  public abstract start(context: Context): Promise<void>;
  public abstract stop(context: Context): Promise<void>;

  public addEndpoint(endpoint: SinkEndpoint): void {
    this.#endpoints.set(endpoint.id, endpoint);
  }

  public endpoint(id: number): SinkEndpoint | undefined {
    return this.#endpoints.get(id);
  }

  public endpoints(): readonly SinkEndpoint[] {
    return [...this.#endpoints.values()];
  }
}

export class DataSinkEndpoint implements SinkEndpoint {
  readonly #consumers: OutputEndpointConsumer[] = [];
  readonly #dataSink: DataSink;
  public readonly id: number;
  public readonly name: string;
  readonly #metrics: DataSinkEndpointMetrics | undefined;

  public constructor(dataSink: DataSink, endpointId: number) {
    const config = dataSink.runtimeEnvironment().runtimeConfig().endpointById(endpointId);
    if (config === undefined) {
      throw new Error(`endpoint config ${String(endpointId)} not found`);
    }
    if (config.idDataConnector !== dataSink.id) {
      throw new Error(
        `endpoint ${config.name} belongs to connector ${String(config.idDataConnector)}, not ${String(dataSink.id)}`
      );
    }
    this.#dataSink = dataSink;
    this.id = endpointId;
    this.name = config.name;
    this.#metrics = makeDataSinkEndpointMetrics(dataSink, this.name);
  }

  public config(): EndpointConfig {
    const config = this.runtimeEnvironment().runtimeConfig().endpointById(this.id);
    if (config === undefined) {
      throw new Error(`endpoint config ${String(this.id)} not found`);
    }
    return config;
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    return this.#dataSink.runtimeEnvironment();
  }

  public dataSink(): DataSink {
    return this.#dataSink;
  }

  public dataConnector(): DataConnector {
    return this.#dataSink;
  }

  public addEndpointConsumer(consumer: OutputEndpointConsumer): void {
    this.#consumers.push(consumer);
  }

  public endpointConsumers(): readonly OutputEndpointConsumer[] {
    return [...this.#consumers];
  }

  public onBeginRequestFailed(context: MessageContext, error: Error): void {
    this.runtimeEnvironment()
      .log()
      .error(context, "BeginRequest failed", str("endpoint", this.name), err(error));
    this.#metrics?.beginRequestFailed.inc(context);
  }

  public onLateResult(context: MessageContext, streamId: string): void {
    this.runtimeEnvironment()
      .log()
      .warn(
        context,
        "late result for sink endpoint",
        str("endpoint", this.name),
        str("stream_id", streamId)
      );
    this.#metrics?.lateResult.inc(context);
  }

  public onRequestStart(context: MessageContext): number | undefined {
    void context;
    if (this.#metrics === undefined) {
      return undefined;
    }
    this.#metrics.activeRequests.inc();
    return performance.now();
  }

  public onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void {
    if (this.#metrics === undefined || started === undefined) {
      return;
    }
    this.#metrics.activeRequests.dec();
    this.#metrics.requestDuration.observe(context, (performance.now() - started) / 1_000);
    if (error === undefined) {
      this.#metrics.messagesTotal.inc(context);
    } else {
      this.#metrics.requestErrors.inc(context);
    }
  }
}

interface DataSinkEndpointMetrics {
  readonly beginRequestFailed: Int64Counter;
  readonly lateResult: Int64Counter;
  readonly requestErrors: Int64Counter;
  readonly messagesTotal: Int64Counter;
  readonly requestDuration: Float64Histogram;
  readonly activeRequests: Int64Gauge;
}

function makeDataSinkEndpointMetrics(
  dataSink: DataSink,
  endpointName: string
): DataSinkEndpointMetrics | undefined {
  const metrics = dataSink.runtimeEnvironment().metrics();
  if (!metrics.enabled()) {
    return undefined;
  }
  const scope = metrics.scope("datasink_endpoint", {
    connector: dataSink.name,
    endpoint: endpointName,
    protocol: dataSink.config().type === DataConnectorType.GRPC ? "grpc" : ""
  });
  const events = scope.counterVec("events_total", "Total number of events in data sink endpoint");
  return {
    beginRequestFailed: events.with({ event: "begin_request_failed" }),
    lateResult: events.with({ event: "late_result" }),
    requestErrors: events.with({ event: "request_error" }),
    messagesTotal: scope.counter(
      "messages_total",
      "Total number of successfully processed messages in data sink endpoint"
    ),
    requestDuration: scope.histogram(
      "request_duration_seconds",
      "Request duration in seconds for data sink endpoint"
    ),
    activeRequests: scope.gauge(
      "active_requests",
      "Number of active requests in data sink endpoint"
    )
  };
}

export class DataSinkEndpointConsumer<T, E> implements OutputEndpointConsumer {
  readonly #endpoint: SinkEndpoint;
  readonly #stream: TypedSinkStream<T, E>;

  public constructor(endpoint: SinkEndpoint, stream: TypedSinkStream<T, E>) {
    this.#endpoint = endpoint;
    this.#stream = stream;
  }

  public endpoint(): SinkEndpoint {
    return this.#endpoint;
  }

  public stream(): TypedSinkStream<T, E> {
    return this.#stream;
  }
}

export class DataSinkEndpointConsumerWithResult<T, R, E> implements OutputEndpointConsumer {
  readonly #endpoint: SinkEndpoint;
  readonly #stream: TypedSinkStreamWithResult<T, R, E>;

  public constructor(endpoint: SinkEndpoint, stream: TypedSinkStreamWithResult<T, R, E>) {
    this.#endpoint = endpoint;
    this.#stream = stream;
  }

  public endpoint(): SinkEndpoint {
    return this.#endpoint;
  }

  public stream(): TypedSinkStreamWithResult<T, R, E> {
    return this.#stream;
  }
}
