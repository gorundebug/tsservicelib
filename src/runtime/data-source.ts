import { performance } from "node:perf_hooks";

import {
  DataConnectorType,
  type DataConnectorConfig,
  type EndpointConfig
} from "./config/index.js";
import type { Collector } from "./collector.js";
import type { Context, MessageContext } from "./context.js";
import { RuntimeDataConnector, type DataConnector, type Endpoint } from "./data-connector.js";
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
import type {
  Completion,
  Consumer,
  TypedConsumedStream,
  TypedStream,
  TypedStreamConsumer
} from "./stream.js";

/** Runtime boundary implemented structurally by the Input operator. */
export interface TypedInputStream<T, R, E> extends TypedStream<T>, TypedStreamConsumer<T> {
  endpointId(): number;
  errorStream(): TypedConsumedStream<E>;
  resultStream(): TypedStream<R> | undefined;
  setSource(source: TypedStream<R>): void;
  setResultConsumer(consumer: Consumer<R>): void;
  consumeError(context: MessageContext, value: E): Completion;
  consumeResult(context: MessageContext, value: R): Completion;
}

/** Common typed collector context passed to datasource endpoint handlers. */
export class StreamContext<T, R, E> {
  public readonly stream: TypedStream<T>;
  public readonly resultStream: TypedStream<R> | undefined;
  public readonly logger: Logger;
  readonly #collector: Collector<T>;
  readonly #errorCollector: Collector<E>;

  public constructor(
    stream: TypedStream<T>,
    resultStream: TypedStream<R> | undefined,
    logger: Logger,
    collector: Collector<T>,
    errorCollector: Collector<E>
  ) {
    this.stream = stream;
    this.resultStream = resultStream;
    this.logger = logger;
    this.#collector = collector;
    this.#errorCollector = errorCollector;
  }

  public collect(context: MessageContext, value: T): Completion {
    return this.#collector.out(context, value);
  }

  public errorCollect(context: MessageContext, value: E): Completion {
    return this.#errorCollector.out(context, value);
  }
}

export function makeStreamContext<T, R, E>(
  stream: TypedStream<T>,
  resultStream: TypedStream<R> | undefined,
  collector: Collector<T>,
  errorCollector: Collector<E>
): StreamContext<T, R, E> {
  return new StreamContext(
    stream,
    resultStream,
    stream.runtimeEnvironment().log(),
    collector,
    errorCollector
  );
}

export interface DataSource extends DataConnector, Lifecycle {
  config(): DataConnectorConfig;
  runtimeEnvironment(): RuntimeEnvironment;
  addEndpoint(endpoint: InputEndpoint): void;
  endpoint(id: number): InputEndpoint | undefined;
  endpoints(): readonly InputEndpoint[];
}

export interface InputEndpoint extends Endpoint {
  dataSource(): DataSource;
  addEndpointConsumer(consumer: InputEndpointConsumer): void;
  endpointConsumers(): readonly InputEndpointConsumer[];
  onMissingStreamId(context: MessageContext): void;
  onLateResult(context: MessageContext, sessionId: string): void;
  onUnknownMessageId(context: MessageContext, sessionId: string, messageId: string): void;
  onDuplicateMessageId(context: MessageContext, sessionId: string, messageId: string): void;
  onPendingAdd(context: MessageContext, streamId: string): void;
  onPendingRemove(context: MessageContext, streamId: string): void;
  onInvalidHttpMethod(context: Context, method: string): void;
  onBeginRequestFailed(context: MessageContext, error: Error): void;
  onRequestStart(context: MessageContext): number | undefined;
  onRequestEnd(context: MessageContext, started: number | undefined, error?: Error): void;
}

export interface InputEndpointConsumer {
  endpoint(): InputEndpoint;
}

export abstract class InputDataSource extends RuntimeDataConnector implements DataSource {
  readonly #endpoints = new Map<number, InputEndpoint>();

  public abstract start(context: Context): Promise<void>;
  public abstract stop(context: Context): Promise<void>;

  public addEndpoint(endpoint: InputEndpoint): void {
    this.#endpoints.set(endpoint.id, endpoint);
  }

  public endpoint(id: number): InputEndpoint | undefined {
    return this.#endpoints.get(id);
  }

  public endpoints(): readonly InputEndpoint[] {
    return [...this.#endpoints.values()];
  }
}

export class DataSourceEndpoint implements InputEndpoint {
  readonly #consumers: InputEndpointConsumer[] = [];
  readonly #dataSource: DataSource;
  public readonly id: number;
  public readonly name: string;
  readonly #metrics: DataSourceEndpointMetrics | undefined;
  readonly #pendingStarted = new Map<string, number>();

  public constructor(dataSource: DataSource, endpointId: number) {
    const config = dataSource.runtimeEnvironment().runtimeConfig().endpointById(endpointId);
    if (config === undefined) {
      throw new Error(`endpoint config ${String(endpointId)} not found`);
    }
    if (config.idDataConnector !== dataSource.id) {
      throw new Error(
        `endpoint ${config.name} belongs to connector ${String(config.idDataConnector)}, not ${String(dataSource.id)}`
      );
    }
    this.#dataSource = dataSource;
    this.id = endpointId;
    this.name = config.name;
    this.#metrics = makeDataSourceEndpointMetrics(dataSource, this.name, () =>
      this.oldestPendingAge()
    );
  }

  public config(): EndpointConfig {
    const config = this.runtimeEnvironment().runtimeConfig().endpointById(this.id);
    if (config === undefined) {
      throw new Error(`endpoint config ${String(this.id)} not found`);
    }
    return config;
  }

  public runtimeEnvironment(): RuntimeEnvironment {
    return this.#dataSource.runtimeEnvironment();
  }

  public dataSource(): DataSource {
    return this.#dataSource;
  }

  public dataConnector(): DataConnector {
    return this.#dataSource;
  }

  public addEndpointConsumer(consumer: InputEndpointConsumer): void {
    this.#consumers.push(consumer);
  }

  public endpointConsumers(): readonly InputEndpointConsumer[] {
    return [...this.#consumers];
  }

  public onMissingStreamId(context: MessageContext): void {
    this.runtimeEnvironment()
      .log()
      .error(context, "consumeResult called without streamID", str("endpoint", this.name));
    this.#metrics?.missingStreamId.inc(context);
  }

  public onLateResult(context: MessageContext, sessionId: string): void {
    this.runtimeEnvironment()
      .log()
      .warn(
        context,
        "consumeResult: session not found in pending",
        str("endpoint", this.name),
        str("session_id", sessionId)
      );
    this.#metrics?.lateResult.inc(context);
  }

  public onUnknownMessageId(context: MessageContext, sessionId: string, messageId: string): void {
    this.runtimeEnvironment()
      .log()
      .warn(
        context,
        "consumeResult: unknown message ID",
        str("endpoint", this.name),
        str("message_id", messageId),
        str("session_id", sessionId)
      );
    this.#metrics?.unknownMessageId.inc(context);
  }

  public onDuplicateMessageId(context: MessageContext, sessionId: string, messageId: string): void {
    this.runtimeEnvironment()
      .log()
      .warn(
        context,
        "consumeResult: duplicate message ID",
        str("endpoint", this.name),
        str("message_id", messageId),
        str("session_id", sessionId)
      );
    this.#metrics?.duplicateMessageId.inc(context);
  }

  public onPendingAdd(context: MessageContext, streamId: string): void {
    void context;
    if (this.#metrics === undefined) {
      return;
    }
    this.#metrics.pendingRequests.inc();
    this.#pendingStarted.set(streamId, performance.now());
  }

  public onPendingRemove(context: MessageContext, streamId: string): void {
    void context;
    if (this.#metrics === undefined) {
      return;
    }
    this.#metrics.pendingRequests.dec();
    this.#pendingStarted.delete(streamId);
  }

  public onInvalidHttpMethod(context: Context, method: string): void {
    this.runtimeEnvironment()
      .log()
      .warn(context, "invalid HTTP method", str("method", method), str("endpoint", this.name));
    this.#metrics?.invalidHttpMethod.inc(context);
  }

  public onBeginRequestFailed(context: MessageContext, error: Error): void {
    this.runtimeEnvironment()
      .log()
      .error(context, "BeginRequest failed", str("endpoint", this.name), err(error));
    this.#metrics?.beginRequestFailed.inc(context);
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

  private oldestPendingAge(): number {
    let oldest = Infinity;
    for (const started of this.#pendingStarted.values()) {
      oldest = Math.min(oldest, started);
    }
    return oldest === Infinity ? 0 : (performance.now() - oldest) / 1_000;
  }
}

interface DataSourceEndpointMetrics {
  readonly missingStreamId: Int64Counter;
  readonly lateResult: Int64Counter;
  readonly unknownMessageId: Int64Counter;
  readonly duplicateMessageId: Int64Counter;
  readonly invalidHttpMethod: Int64Counter;
  readonly beginRequestFailed: Int64Counter;
  readonly requestErrors: Int64Counter;
  readonly messagesTotal: Int64Counter;
  readonly requestDuration: Float64Histogram;
  readonly activeRequests: Int64Gauge;
  readonly pendingRequests: Int64Gauge;
}

function makeDataSourceEndpointMetrics(
  dataSource: DataSource,
  endpointName: string,
  oldestPendingAge: () => number
): DataSourceEndpointMetrics | undefined {
  const metrics = dataSource.runtimeEnvironment().metrics();
  if (!metrics.enabled()) {
    return undefined;
  }
  const scope = metrics.scope("datasource_endpoint", {
    connector: dataSource.name,
    endpoint: endpointName,
    protocol: dataSource.config().type === DataConnectorType.GRPC ? "grpc" : ""
  });
  const events = scope.counterVec("events_total", "Total number of events in data source endpoint");
  scope.observableFloat64Gauge(
    "pending_oldest_age_seconds",
    "Age in seconds of the oldest pending request awaiting a pipeline result",
    oldestPendingAge
  );
  return {
    missingStreamId: events.with({ event: "missing_stream_id" }),
    lateResult: events.with({ event: "late_result" }),
    unknownMessageId: events.with({ event: "unknown_message_id" }),
    duplicateMessageId: events.with({ event: "duplicate_message_id" }),
    invalidHttpMethod: events.with({ event: "invalid_http_method" }),
    beginRequestFailed: events.with({ event: "begin_request_failed" }),
    requestErrors: events.with({ event: "request_error" }),
    messagesTotal: scope.counter(
      "messages_total",
      "Total number of successfully processed messages in data source endpoint"
    ),
    requestDuration: scope.histogram(
      "request_duration_seconds",
      "Request duration in seconds for data source endpoint"
    ),
    activeRequests: scope.gauge(
      "active_requests",
      "Number of active requests in data source endpoint"
    ),
    pendingRequests: scope.gauge(
      "pending_requests",
      "Number of requests awaiting a pipeline result"
    )
  };
}

export class DataSourceEndpointConsumer<T, R, E> implements InputEndpointConsumer {
  readonly #endpoint: InputEndpoint;
  readonly #stream: TypedInputStream<T, R, E>;

  public constructor(endpoint: InputEndpoint, stream: TypedInputStream<T, R, E>) {
    this.#endpoint = endpoint;
    this.#stream = stream;
  }

  public endpoint(): InputEndpoint {
    return this.#endpoint;
  }

  public stream(): TypedInputStream<T, R, E> {
    return this.#stream;
  }

  public consume(context: MessageContext, value: T): void | Promise<void> {
    return this.#stream.consume(context, value);
  }
}
