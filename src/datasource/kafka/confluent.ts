import { createRequire } from "node:module";

import type { KafkaJS } from "@confluentinc/kafka-javascript";

import {
  applyDataSourceEndpointTracing,
  DataSourceEndpoint,
  DataSourceEndpointConsumer,
  FunctionCollector,
  InputDataSource,
  Context,
  MessageContext,
  RotatingMap,
  RuntimeTaskRegistry,
  TRACE_SAMPLING_HEADER,
  boolAttribute,
  err,
  errorFromUnknown,
  makeStreamContext,
  newStreamId,
  requireKafkaDataConnectorConfig,
  requireKafkaEndpointConfig,
  spanError,
  stringAttribute,
  type Completion,
  type Consumer,
  type InputEndpointConsumer,
  type KafkaDataConnectorConfig,
  type Metrics,
  type RuntimeEnvironment,
  type Span,
  type StreamContext,
  type Tracer,
  type TypedInputStream
} from "../../runtime/index.js";
import { librdkafkaStatisticsOptions } from "../../runtime/telemetry/librdkafka-statistics.js";

const PENDING_ROTATION_INTERVAL_MS = 30_000;
const RECONNECT_DELAY_MS = 100;
const require = createRequire(import.meta.url);
let confluentKafka: typeof import("@confluentinc/kafka-javascript") | undefined;

function kafkaJS(): typeof KafkaJS {
  confluentKafka ??=
    require("@confluentinc/kafka-javascript") as typeof import("@confluentinc/kafka-javascript");
  return confluentKafka.KafkaJS;
}

export interface KafkaRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: bigint;
  readonly key: Uint8Array | undefined;
  readonly value: Uint8Array;
  readonly headers: ReadonlyMap<string, Uint8Array>;
}

export interface KafkaConsumerControl {
  mark(record: KafkaRecord, metadata: string): void;
  commit(record: KafkaRecord): Promise<void>;
}

export class ConsumerMessage {
  public readonly key: Uint8Array | undefined;
  public readonly value: Uint8Array;
  public readonly topic: string;
  public readonly partition: number;
  public readonly offset: bigint;
  readonly #record: KafkaRecord;
  readonly #control: KafkaConsumerControl;

  public constructor(record: KafkaRecord, control: KafkaConsumerControl) {
    this.#record = record;
    this.#control = control;
    this.key = record.key;
    this.value = record.value;
    this.topic = record.topic;
    this.partition = record.partition;
    this.offset = record.offset;
  }

  public markMessage(metadata = ""): void {
    this.#control.mark(this.#record, metadata);
  }

  public commit(): Promise<void> {
    return this.#control.commit(this.#record);
  }
}

export interface KafkaConsumer {
  connect(): Promise<void>;
  subscribe(topic: string): Promise<void>;
  run(
    concurrency: number,
    handler: (record: KafkaRecord, control: KafkaConsumerControl) => Promise<void>
  ): Promise<void>;
  stop(): Promise<void>;
  disconnect(): Promise<void>;
}

export interface KafkaAdmin {
  connect(): Promise<void>;
  createTopic(topic: string, partitions: number, replicationFactor: number): Promise<void>;
  partitionCount(topic: string): Promise<number>;
  disconnect(): Promise<void>;
}

export interface KafkaClientFactory {
  consumer(
    brokers: readonly string[],
    groupId: string,
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaConsumer;
  admin(
    brokers: readonly string[],
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaAdmin;
}

interface KafkaSecurity {
  readonly protocol: "PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL";
  readonly mechanism: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
  readonly username?: string | undefined;
  readonly password?: string | undefined;
}

export class ConfluentKafkaClientFactory implements KafkaClientFactory {
  readonly #metrics: Metrics | undefined;

  public constructor(metrics?: Metrics) {
    this.#metrics = metrics;
  }

  public consumer(
    brokers: readonly string[],
    groupId: string,
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaConsumer {
    return new ConfluentConsumer(
      makeKafka(brokers, connectionTimeoutMs, this.#metrics, security).consumer({
        kafkaJS: { groupId, fromBeginning: true, autoCommit: true }
      })
    );
  }

  public admin(
    brokers: readonly string[],
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaAdmin {
    return new ConfluentAdmin(makeKafka(brokers, connectionTimeoutMs, undefined, security).admin());
  }
}

const defaultKafkaClientFactories = new WeakMap<RuntimeEnvironment, KafkaClientFactory>();

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
    message: ConsumerMessage,
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

class KafkaResult<HandlerState, T, R, E> implements ResultContext<HandlerState, T, R, E> {
  public readonly state: HandlerState;
  readonly #span: Span | undefined;
  readonly #recordDone: boolean;
  readonly #callbacks = new Map<string, ResultCallback<HandlerState, T, R, E>>();
  readonly #done: Promise<void>;
  #resolveDone: (() => void) | undefined;
  #completed = false;
  #retiring = false;
  #activeCallbacks = 0;
  #retired: Promise<void> | undefined;
  #resolveRetired: (() => void) | undefined;

  public constructor(state: HandlerState, span: Span | undefined, recordDone: boolean) {
    this.state = state;
    this.#span = span;
    this.#recordDone = recordDone;
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
    if (this.#recordDone) this.#span?.addEvent("done_called");
    this.#resolveDone?.();
    this.#resolveDone = undefined;
  }

  public wait(): Promise<void> {
    return this.#done;
  }

  public span(): Span | undefined {
    return this.#span;
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

class KafkaSourceEndpoint extends DataSourceEndpoint {
  readonly topic: string;
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly createTopic: boolean;
  readonly consumerGroup: string;
  #partitionCount = 1;
  #consumer: KafkaConsumer | undefined;
  #binding: KafkaEndpointConsumerContract | undefined;
  #run: Promise<void> | undefined;
  #reconnect: AbortController | undefined;
  #admissionStopped = false;

  public constructor(dataSource: KafkaDataSource, endpointId: number) {
    super(dataSource, endpointId);
    const config = requireKafkaEndpointConfig(this.config());
    this.topic = config.topic;
    this.partitions = config.partitions === 0 ? 1 : config.partitions;
    this.replicationFactor = config.replicationFactor === 0 ? 1 : config.replicationFactor;
    this.createTopic = config.createTopic;
    this.consumerGroup = config.consumerGroup;
  }

  public bind(binding: KafkaEndpointConsumerContract): void {
    if (this.#binding !== undefined)
      throw new Error(`consumer already assigned to Kafka endpoint ${this.name}`);
    this.#binding = binding;
    this.addEndpointConsumer(binding);
  }

  public enabled(): boolean {
    return requireKafkaEndpointConfig(this.config()).enabled;
  }

  public validate(): void {
    if (this.topic.length === 0)
      throw new Error(`no topic specified for Kafka endpoint ${this.name}`);
    if (this.consumerGroup.length === 0)
      throw new Error(`no consumer group specified for Kafka endpoint ${this.name}`);
  }

  public setPartitionCount(partitionCount: number): void {
    if (!Number.isSafeInteger(partitionCount) || partitionCount < 1) {
      throw new RangeError(`Kafka endpoint ${this.name} has invalid broker partition count`);
    }
    this.#partitionCount = partitionCount;
  }

  public async start(context: Context): Promise<void> {
    if (!this.enabled()) return;
    this.validate();
    const reconnect = new AbortController();
    const consumer = await this.connectCurrentConsumer();
    try {
      this.#consumer = consumer;
      this.#reconnect = reconnect;
      this.#admissionStopped = false;
      await this.#binding?.start();
      this.#run = this.supervise(context, consumer, reconnect.signal);
    } catch (error: unknown) {
      this.#consumer = undefined;
      this.#reconnect = undefined;
      await consumer.disconnect();
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    await this.stopAdmission(context);
    await this.#binding?.stop(context);
    await this.#run;
    this.#consumer = undefined;
    this.#run = undefined;
  }

  public async stopAdmission(context: Context): Promise<void> {
    void context;
    if (this.#admissionStopped) return;
    this.#admissionStopped = true;
    this.#reconnect?.abort(new Error(`Kafka endpoint ${this.name} stopped`));
    this.#reconnect = undefined;
    this.#binding?.stopAdmission();
    const consumer = this.#consumer;
    if (consumer !== undefined) {
      await consumer.stop();
    }
  }

  private async supervise(
    context: Context,
    initial: KafkaConsumer,
    signal: AbortSignal
  ): Promise<void> {
    let consumer: KafkaConsumer | undefined = initial;
    for (;;) {
      if (consumer === undefined) {
        if (!(await reconnectDelay(signal))) return;
        try {
          consumer = await this.connectCurrentConsumer();
          if (signal.aborted) {
            await consumer.disconnect();
            return;
          }
          this.#consumer = consumer;
        } catch (error: unknown) {
          this.logReconnectFailure(context, error);
          continue;
        }
      }
      try {
        await consumer.run(this.#partitionCount, async (record, control) =>
          this.#binding?.handle(record, control)
        );
        if (!signal.aborted) {
          this.logReconnectFailure(context, new Error("Kafka consumer stopped unexpectedly"));
        }
      } catch (error: unknown) {
        if (!signal.aborted) this.logReconnectFailure(context, error);
      } finally {
        if (this.#consumer === consumer) this.#consumer = undefined;
        await consumer.disconnect();
        consumer = undefined;
      }
      if (signal.aborted) return;
    }
  }

  private async connectCurrentConsumer(): Promise<KafkaConsumer> {
    const dataSource = this.dataSource();
    if (!(dataSource instanceof KafkaDataSource)) {
      throw new Error(`invalid Kafka data source for ${this.name}`);
    }
    const config = requireKafkaDataConnectorConfig(dataSource.config());
    const brokers = splitBrokers(config.brokers, dataSource.name);
    const consumer = dataSource
      .factory()
      .consumer(brokers, this.consumerGroup, config.dialTimeout, kafkaSecurity(config));
    await consumer.connect();
    try {
      await consumer.subscribe(this.topic);
      return consumer;
    } catch (error: unknown) {
      await consumer.disconnect();
      throw error;
    }
  }

  private logReconnectFailure(context: Context, error: unknown): void {
    this.runtimeEnvironment()
      .log()
      .error(context, "Kafka consumer reconnect required", err(errorFromUnknown(error)));
  }
}

interface KafkaEndpointConsumerContract extends InputEndpointConsumer {
  start(): Promise<void>;
  stopAdmission(): void;
  stop(context: Context): Promise<void>;
  handle(record: KafkaRecord, control: KafkaConsumerControl): Promise<void>;
}

export class KafkaDataSource extends InputDataSource {
  readonly #factory: KafkaClientFactory;
  #started = false;

  public constructor(
    connectorId: number,
    environment: RuntimeEnvironment,
    factory: KafkaClientFactory
  ) {
    super(connectorId, environment);
    requireKafkaDataConnectorConfig(this.config());
    this.#factory = factory;
  }

  public factory(): KafkaClientFactory {
    return this.#factory;
  }

  public async start(context: Context): Promise<void> {
    void context;
    if (this.#started) throw new Error(`Kafka data source ${this.name} is already started`);
    this.#started = true;
    const endpoints = this.kafkaEndpoints();
    const enabled = endpoints.filter((endpoint) => endpoint.enabled());
    if (enabled.length === 0) return;
    const config = requireKafkaDataConnectorConfig(this.config());
    const brokers = splitBrokers(config.brokers, this.name);
    const admin = this.#factory.admin(brokers, config.dialTimeout, kafkaSecurity(config));
    try {
      let adminConnected = false;
      try {
        for (const endpoint of enabled) endpoint.validate();
        await admin.connect();
        adminConnected = true;
        for (const endpoint of enabled) {
          if (endpoint.createTopic) {
            await admin.createTopic(
              endpoint.topic,
              endpoint.partitions,
              endpoint.replicationFactor
            );
          }
          endpoint.setPartitionCount(await admin.partitionCount(endpoint.topic));
        }
      } finally {
        if (adminConnected) await admin.disconnect();
      }
    } catch (error: unknown) {
      this.#started = false;
      throw error;
    }
    try {
      for (const endpoint of enabled) await endpoint.start(context);
    } catch (error: unknown) {
      this.#started = false;
      await Promise.allSettled(
        enabled.map(async (endpoint) => endpoint.stop(Context.background()))
      );
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) return;
    this.#started = false;
    await Promise.all(this.kafkaEndpoints().map(async (endpoint) => endpoint.stop(context)));
  }

  public async stopAdmission(context: Context): Promise<void> {
    if (!this.#started) return;
    await Promise.all(
      this.kafkaEndpoints().map(async (endpoint) => endpoint.stopAdmission(context))
    );
  }

  private kafkaEndpoints(): readonly KafkaSourceEndpoint[] {
    return this.endpoints().map((endpoint) => {
      if (!(endpoint instanceof KafkaSourceEndpoint))
        throw new Error(`source endpoint ${endpoint.name} is not Kafka`);
      return endpoint;
    });
  }
}

class KafkaEndpointConsumer<HandlerState, T, R, E>
  extends DataSourceEndpointConsumer<T, R, E>
  implements KafkaEndpointConsumerContract
{
  readonly #streamContext: StreamContext<T, R, E>;
  readonly #handler: EndpointHandler<HandlerState, T, R, E>;
  readonly #tasks = new RuntimeTaskRegistry();
  #pending: RotatingMap<string, KafkaResult<HandlerState, T, R, E>> | undefined;
  readonly #waiters: (() => void)[] = [];
  readonly #tracer: Tracer | undefined;
  #active = 0;
  #started = false;
  #stopped = false;

  public constructor(
    endpoint: KafkaSourceEndpoint,
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
    stream.setResultConsumer({
      consume: (context, value) => this.consumeResult(context, value)
    });
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public start(): Promise<void> {
    if (this.#started)
      return Promise.reject(new Error(`Kafka endpoint ${this.endpoint().name} already started`));
    this.#started = true;
    this.#stopped = false;
    if (this.stream().resultStream() !== undefined) {
      this.#pending = new RotatingMap(PENDING_ROTATION_INTERVAL_MS);
      this.#pending.start(Context.background());
    }
    return Promise.resolve();
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started && !this.#stopped) return;
    this.stopAdmission();
    try {
      await this.#tasks.drain(context.remainingMs());
    } catch (error: unknown) {
      this.#tasks.cancel(error);
      throw error;
    } finally {
      this.#pending?.stop(context);
    }
  }

  public stopAdmission(): void {
    if (!this.#started) return;
    this.#started = false;
    this.#stopped = true;
    for (const wake of this.#waiters.splice(0)) wake();
    this.#tasks.stopAdmission();
  }

  public override consume(context: MessageContext, value: T): Completion {
    void context;
    void value;
  }

  public handle(record: KafkaRecord, control: KafkaConsumerControl): Promise<void> {
    if (!this.#started) return Promise.resolve();
    return this.#tasks.admit(async (signal) => this.handleOnce(record, control, signal));
  }

  private async handleOnce(
    record: KafkaRecord,
    control: KafkaConsumerControl,
    signal: AbortSignal
  ): Promise<void> {
    await this.acquire(signal);
    try {
      await this.handleAdmitted(record, control, signal);
    } finally {
      this.#active -= 1;
      this.#waiters.shift()?.();
    }
  }

  private async handleAdmitted(
    record: KafkaRecord,
    control: KafkaConsumerControl,
    signal: AbortSignal
  ): Promise<void> {
    let context = new MessageContext().withExternalCancellation(signal);
    const metadata = new Map<string, string>();
    for (const [name, value] of record.headers) {
      if ([TRACE_SAMPLING_HEADER, "traceparent", "tracestate", "baggage"].includes(name)) {
        metadata.set(name, Buffer.from(value).toString("utf8"));
      }
    }
    context = applyDataSourceEndpointTracing(
      context.withMetadata(metadata),
      this.stream().runtimeEnvironment(),
      this.endpoint().id
    );
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "kafka.input", [
        stringAttribute("stream", this.stream().name),
        stringAttribute("endpoint", this.endpoint().name)
      ]);
      context = started.context;
      span = started.span;
    }
    try {
      await this.handleTraced(record, control, context, span);
    } finally {
      span?.end();
    }
  }

  private async handleTraced(
    record: KafkaRecord,
    control: KafkaConsumerControl,
    context: MessageContext,
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
    const hasResult = this.stream().resultStream() !== undefined;
    span?.setAttributes([
      stringAttribute("stream_id", streamId),
      boolAttribute("has_result", hasResult)
    ]);
    const result = new KafkaResult<HandlerState, T, R, E>(state, span, hasResult);
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
      await this.#handler.consumeMessage(
        context,
        this.#streamContext,
        state,
        new ConsumerMessage(record, control),
        result
      );
      span?.addEvent("consume_message");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
    }
    if (failure === undefined) {
      if (!hasResult) result.done();
      try {
        await waitForResult(result, context.signal());
        if (hasResult) span?.addEvent("done_received");
      } catch (error: unknown) {
        failure = errorFromUnknown(error);
        resultWaitFailed = true;
      }
    }
    if (hasResult) {
      const resultCompleted = await result.retire();
      if (resultWaitFailed && resultCompleted) failure = undefined;
      this.pending().pop(streamId);
      this.endpoint().onPendingRemove(context, streamId);
    }
    if (failure !== undefined) spanError(span, failure);
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
          ? new Error("Kafka endpoint stopped")
          : errorFromUnknown(signal.reason);
      }
      const concurrency = this.#handler.concurrency(this.#streamContext);
      if (concurrency < 0 || !Number.isSafeInteger(concurrency)) {
        throw new RangeError("Kafka endpoint concurrency must be a non-negative safe integer");
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
    if (!found || result === undefined) {
      this.endpoint().onLateResult(context, streamId);
      return;
    }
    if (!result.beginCallback()) {
      this.endpoint().onLateResult(context, streamId);
      result.span()?.addEvent("late_result");
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
        result.span()?.addEvent("unknown_message_id", [stringAttribute("message_id", messageId)]);
        return;
      }
      if (await callback(context, this.#streamContext, result.state, value)) {
        if (!result.remove(messageId, callback)) {
          this.endpoint().onDuplicateMessageId(context, streamId, messageId);
          result
            .span()
            ?.addEvent("duplicate_message_id", [stringAttribute("message_id", messageId)]);
        }
      }
      result.span()?.addEvent("result_consumed", [stringAttribute("message_id", messageId)]);
    } finally {
      result.endCallback();
    }
  }

  private pending(): RotatingMap<string, KafkaResult<HandlerState, T, R, E>> {
    if (this.#pending === undefined) {
      throw new Error(`Kafka endpoint ${this.endpoint().name} pending store is not started`);
    }
    return this.#pending;
  }
}

export function makeKafkaEndpointConsumer<HandlerState, T, R, E>(
  stream: TypedInputStream<T, R, E>,
  handler: EndpointHandler<HandlerState, T, R, E>,
  factory?: KafkaClientFactory
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  factory ??= defaultKafkaClientFactory(environment);
  const endpointConfig = requireKafkaEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment, factory);
  if (dataSource.endpoint(endpointConfig.id) !== undefined)
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  const endpoint = new KafkaSourceEndpoint(dataSource, endpointConfig.id);
  const consumer = new KafkaEndpointConsumer(endpoint, stream, handler);
  endpoint.bind(consumer);
  dataSource.addEndpoint(endpoint);
  return consumer;
}

function getOrCreateDataSource(
  connectorId: number,
  environment: RuntimeEnvironment,
  factory: KafkaClientFactory
): KafkaDataSource {
  const existing = environment.dataSourceById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof KafkaDataSource))
      throw new Error(`data source ${String(connectorId)} is not Kafka`);
    if (existing.factory() !== factory)
      throw new Error(`Kafka data source ${existing.name} already uses another factory`);
    return existing;
  }
  const dataSource = new KafkaDataSource(connectorId, environment, factory);
  environment.addDataSource(dataSource);
  return dataSource;
}

function splitBrokers(value: string, connectorName: string): readonly string[] {
  const brokers = value
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
  if (brokers.length === 0)
    throw new Error(`no brokers specified for Kafka data connector ${connectorName}`);
  return brokers;
}

async function reconnectDelay(signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return false;
  return await new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", cancel);
      resolve(true);
    }, RECONNECT_DELAY_MS);
    const cancel = (): void => {
      clearTimeout(timer);
      resolve(false);
    };
    signal.addEventListener("abort", cancel, { once: true });
  });
}

async function waitForResult<HandlerState, T, R, E>(
  result: KafkaResult<HandlerState, T, R, E>,
  signal: AbortSignal
): Promise<void> {
  if (signal.aborted) {
    throw signal.reason === undefined
      ? new Error("Kafka request cancelled")
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
              ? new Error("Kafka request cancelled")
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

function makeKafka(
  brokers: readonly string[],
  connectionTimeoutMs: number,
  metrics?: Metrics,
  security?: KafkaSecurity
): KafkaJS.Kafka {
  const statistics =
    metrics === undefined ? undefined : librdkafkaStatisticsOptions(metrics, "consumer");
  return new (kafkaJS().Kafka)({
    ...statistics,
    kafkaJS: {
      brokers: [...brokers],
      ...kafkaSecurityOptions(security),
      ...(connectionTimeoutMs === 0 ? {} : { connectionTimeout: connectionTimeoutMs })
    }
  });
}

function kafkaSecurity(config: KafkaDataConnectorConfig): KafkaSecurity {
  return {
    protocol: config.securityProtocol,
    mechanism: config.saslMechanism,
    username: config.username,
    password: config.password
  };
}

function kafkaSecurityOptions(security?: KafkaSecurity): Pick<KafkaJS.KafkaConfig, "ssl" | "sasl"> {
  if (security === undefined || security.protocol === "PLAINTEXT") return {};
  if (
    security.username === undefined ||
    security.username === "" ||
    security.password === undefined ||
    security.password === ""
  ) {
    throw new Error("Kafka SASL username and password must both be configured");
  }
  return {
    ssl: security.protocol === "SASL_SSL",
    sasl: {
      mechanism: security.mechanism.toLowerCase() as "plain" | "scram-sha-256" | "scram-sha-512",
      username: security.username,
      password: security.password
    }
  };
}

function defaultKafkaClientFactory(environment: RuntimeEnvironment): KafkaClientFactory {
  const existing = defaultKafkaClientFactories.get(environment);
  if (existing !== undefined) return existing;
  const factory = new ConfluentKafkaClientFactory(environment.metrics());
  defaultKafkaClientFactories.set(environment, factory);
  return factory;
}

class ConfluentConsumer implements KafkaConsumer {
  readonly #consumer: KafkaJS.Consumer;
  #finishRun: (() => void) | undefined;
  #disconnecting: Promise<void> | undefined;
  public constructor(consumer: KafkaJS.Consumer) {
    this.#consumer = consumer;
  }
  public connect(): Promise<void> {
    return this.#consumer.connect();
  }
  public disconnect(): Promise<void> {
    this.#disconnecting ??= this.disconnectOnce();
    return this.#disconnecting;
  }
  public subscribe(topic: string): Promise<void> {
    return this.#consumer.subscribe({ topic });
  }
  public stop(): Promise<void> {
    // KafkaJS stop followed by disconnect performs two coordinated group
    // shutdowns. Closing the consumer once stops admission and releases the
    // run loop; the supervisor observes the same idempotent promise.
    return this.disconnect();
  }

  private async disconnectOnce(): Promise<void> {
    try {
      await this.#consumer.disconnect();
    } finally {
      this.finishRun();
    }
  }
  public async run(
    concurrency: number,
    handler: (record: KafkaRecord, control: KafkaConsumerControl) => Promise<void>
  ): Promise<void> {
    if (this.#finishRun !== undefined) throw new Error("Kafka consumer is already running");
    let finish!: () => void;
    const lifetime = new Promise<void>((resolve) => {
      finish = resolve;
    });
    this.#finishRun = finish;
    try {
      await this.#consumer.run({
        partitionsConsumedConcurrently: Math.max(1, concurrency),
        eachBatchAutoResolve: false,
        eachBatch: async (payload) => {
          const { batch } = payload;
          for (const message of batch.messages) {
            if (!payload.isRunning() || payload.isStale()) break;
            const record = kafkaRecord(batch.topic, batch.partition, message);
            const control = new ConfluentMessageControl(
              this.#consumer,
              record,
              (offset) => {
                payload.resolveOffset(offset);
              },
              async () => payload.commitOffsetsIfNecessary()
            );
            await handler(record, control);
            await control.complete();
            await payload.heartbeat();
          }
        }
      });
      await lifetime;
    } finally {
      if (this.#finishRun === finish) this.#finishRun = undefined;
    }
  }

  private finishRun(): void {
    this.#finishRun?.();
  }
}

/**
 * The KafkaJS-compatible runtime stores an eachMessage offset only when its
 * callback completes successfully. Keep that decision local to the message:
 * MarkMessage permits the callback to complete, while an unmarked message is
 * rejected so the client seeks it again. The published 1.9 runtime does not
 * implement its declared storeOffsets method; non-empty metadata is therefore
 * committed explicitly instead of being silently discarded.
 */
class ConfluentMessageControl implements KafkaConsumerControl {
  readonly #consumer: KafkaJS.Consumer;
  readonly #record: KafkaRecord;
  readonly #resolveOffset: (offset: string) => void;
  readonly #commitOffsetsIfNecessary: () => Promise<void>;
  #marked = false;
  #committed = false;
  #metadata = "";

  public constructor(
    consumer: KafkaJS.Consumer,
    record: KafkaRecord,
    resolveOffset: (offset: string) => void,
    commitOffsetsIfNecessary: () => Promise<void>
  ) {
    this.#consumer = consumer;
    this.#record = record;
    this.#resolveOffset = resolveOffset;
    this.#commitOffsetsIfNecessary = commitOffsetsIfNecessary;
  }

  public mark(record: KafkaRecord, metadata: string): void {
    this.requireRecord(record);
    this.#marked = true;
    this.#metadata = metadata;
  }

  public async commit(record: KafkaRecord): Promise<void> {
    this.requireRecord(record);
    await this.#consumer.commitOffsets([nextOffset(record)]);
    this.#committed = true;
  }

  public async complete(): Promise<void> {
    if (this.#committed) return;
    if (!this.#marked) return;
    if (this.#metadata.length > 0) {
      await this.#consumer.commitOffsets([nextOffset(this.#record, this.#metadata)]);
      return;
    }
    this.#resolveOffset(String(this.#record.offset));
    await this.#commitOffsetsIfNecessary();
  }

  private requireRecord(record: KafkaRecord): void {
    if (record !== this.#record) {
      throw new Error("Kafka consumer control belongs to another message");
    }
  }
}

class ConfluentAdmin implements KafkaAdmin {
  readonly #admin: KafkaJS.Admin;
  public constructor(admin: KafkaJS.Admin) {
    this.#admin = admin;
  }
  public connect(): Promise<void> {
    return this.#admin.connect();
  }
  public disconnect(): Promise<void> {
    return this.#admin.disconnect();
  }
  public async createTopic(
    topic: string,
    partitions: number,
    replicationFactor: number
  ): Promise<void> {
    await this.#admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor }]
    });
  }

  public async partitionCount(topic: string): Promise<number> {
    const metadata: unknown = await this.#admin.fetchTopicMetadata({ topics: [topic] });
    const topicMetadata = metadataTopics(metadata).find((candidate) => candidate.name === topic);
    if (topicMetadata === undefined) throw new Error(`Kafka topic ${topic} metadata was not found`);
    return topicMetadata.partitions.length;
  }
}

function metadataTopics(
  value: unknown
): readonly { readonly name: string; readonly partitions: readonly unknown[] }[] {
  let rawTopics: unknown;
  if (Array.isArray(value)) rawTopics = value;
  else if (typeof value === "object" && value !== null && "topics" in value) {
    rawTopics = value.topics;
  }
  if (!Array.isArray(rawTopics)) throw new TypeError("Kafka topic metadata has an invalid shape");
  const topics: readonly unknown[] = rawTopics;
  return topics.map((topic) => {
    if (
      typeof topic !== "object" ||
      topic === null ||
      !("name" in topic) ||
      typeof topic.name !== "string" ||
      !("partitions" in topic) ||
      !Array.isArray(topic.partitions)
    ) {
      throw new TypeError("Kafka topic metadata entry has an invalid shape");
    }
    return { name: topic.name, partitions: topic.partitions };
  });
}

function kafkaRecord(topic: string, partition: number, message: KafkaJS.KafkaMessage): KafkaRecord {
  const headers = new Map<string, Uint8Array>();
  for (const [name, raw] of Object.entries(message.headers ?? {})) {
    const first = Array.isArray(raw) ? raw[0] : raw;
    if (first !== undefined) headers.set(name, Buffer.from(first));
  }
  return {
    topic,
    partition,
    offset: BigInt(message.offset),
    key: message.key === null ? undefined : new Uint8Array(message.key),
    value: message.value === null ? new Uint8Array() : new Uint8Array(message.value),
    headers
  };
}

function nextOffset(
  record: KafkaRecord,
  metadata?: string
): KafkaJS.TopicPartitionOffsetAndMetadata {
  return {
    topic: record.topic,
    partition: record.partition,
    offset: String(record.offset + 1n),
    ...(metadata === undefined ? {} : { metadata })
  };
}
