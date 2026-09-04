import { createRequire } from "node:module";

import type { KafkaJS } from "@confluentinc/kafka-javascript";

import {
  DataSinkEndpoint,
  DataSinkEndpointConsumer,
  OutputDataSink,
  RuntimeTaskRegistry,
  err,
  errorFromUnknown,
  requireKafkaDataConnectorConfig,
  requireKafkaEndpointConfig,
  spanError,
  stringAttribute,
  type Completion,
  type Consumer,
  type Context,
  type KafkaDataConnectorConfig,
  type MessageContext,
  type Metrics,
  type RuntimeEnvironment,
  type SinkEndpoint,
  type Span,
  type Stream,
  type Tracer,
  type TypedSinkStream
} from "../../runtime/index.js";
import { librdkafkaStatisticsOptions } from "../../runtime/telemetry/librdkafka-statistics.js";

const require = createRequire(import.meta.url);
let confluentKafka: typeof import("@confluentinc/kafka-javascript") | undefined;

function kafkaJS(): typeof KafkaJS {
  confluentKafka ??=
    require("@confluentinc/kafka-javascript") as typeof import("@confluentinc/kafka-javascript");
  return confluentKafka.KafkaJS;
}

export interface DeliveryResult {
  readonly partition: number;
  readonly offset: bigint;
}

export interface KafkaProducer {
  connect(): Promise<void>;
  send(
    topic: string,
    key: Uint8Array | undefined,
    value: Uint8Array,
    partition?: number,
    headers?: ReadonlyMap<string, string>
  ): Promise<DeliveryResult>;
  flush(timeoutMs?: number): Promise<void>;
  disconnect(): Promise<void>;
}

export interface KafkaAdmin {
  connect(): Promise<void>;
  createTopic(topic: string, partitions: number, replicationFactor: number): Promise<void>;
  partitionCount(topic: string): Promise<number>;
  disconnect(): Promise<void>;
}

export interface KafkaClientFactory {
  producer(
    brokers: readonly string[],
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaProducer;
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

  public producer(
    brokers: readonly string[],
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaProducer {
    const kafka = makeKafka(brokers, connectionTimeoutMs, this.#metrics, security);
    return new ConfluentProducer(kafka.producer());
  }

  public admin(
    brokers: readonly string[],
    connectionTimeoutMs: number,
    security?: KafkaSecurity
  ): KafkaAdmin {
    const kafka = makeKafka(brokers, connectionTimeoutMs, undefined, security);
    return new ConfluentAdmin(kafka.admin());
  }
}

const defaultKafkaClientFactories = new WeakMap<RuntimeEnvironment, KafkaClientFactory>();

export interface Partitioner<T> {
  partition(value: Readonly<T>, partitions: number): number | Promise<number>;
}

export type DeliveryCallback<R> = (
  partition: number,
  offset: bigint,
  error: Error | undefined
) => R | undefined;

export class SinkMessage<R> {
  public key: Uint8Array | undefined;
  public value: Uint8Array = new Uint8Array();
  readonly #topic: string;
  readonly #send: (
    key: Uint8Array | undefined,
    value: Uint8Array,
    onDelivery: (result: DeliveryResult | undefined, error: Error | undefined) => Completion
  ) => void;
  readonly #result: (context: MessageContext, value: R) => Completion;

  public constructor(
    topic: string,
    send: (
      key: Uint8Array | undefined,
      value: Uint8Array,
      onDelivery: (result: DeliveryResult | undefined, error: Error | undefined) => Completion
    ) => void,
    result: (context: MessageContext, value: R) => Completion
  ) {
    this.#topic = topic;
    this.#send = send;
    this.#result = result;
  }

  public topic(): string {
    return this.#topic;
  }

  public send(context: MessageContext, onDelivery: DeliveryCallback<R>): void {
    this.#send(this.key, this.value, async (delivery, error) => {
      const result = onDelivery(delivery?.partition ?? 0, delivery?.offset ?? 0n, error);
      if (result !== undefined) await this.#result(context, result);
    });
  }

  public sendSync(context: MessageContext): Promise<DeliveryResult> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const aborted = () => {
        if (settled) return;
        settled = true;
        reject(
          context.signal().reason === undefined
            ? new Error("Kafka send cancelled")
            : errorFromUnknown(context.signal().reason)
        );
      };
      context.signal().addEventListener("abort", aborted, { once: true });
      try {
        this.#send(this.key, this.value, (delivery, error) => {
          if (settled) return;
          settled = true;
          context.signal().removeEventListener("abort", aborted);
          if (error !== undefined) reject(error);
          else if (delivery !== undefined) resolve(delivery);
          else reject(new Error("Kafka delivery completed without a result"));
        });
      } catch (error: unknown) {
        settled = true;
        context.signal().removeEventListener("abort", aborted);
        reject(errorFromUnknown(error));
        return;
      }
      if (context.cancelled()) aborted();
    });
  }

  public out(context: MessageContext, value: R): Completion {
    return this.#result(context, value);
  }

  public skip(context: MessageContext, value: R): Completion {
    return this.#result(context, value);
  }
}

export interface EndpointHandler<HandlerState, T, R> {
  getStreamId(context: MessageContext, value: Readonly<T>): string;
  beginRequest(
    context: MessageContext,
    stream: Stream
  ):
    | { readonly context: MessageContext; readonly state: HandlerState }
    | Promise<{ readonly context: MessageContext; readonly state: HandlerState }>;
  consumeMessage(
    context: MessageContext,
    stream: Stream,
    handlerState: HandlerState,
    value: Readonly<T>,
    message: SinkMessage<R>
  ): Completion;
  endRequest(
    context: MessageContext,
    stream: Stream,
    error: Error | undefined,
    handlerState: HandlerState
  ): Completion;
}

class KafkaSinkEndpoint extends DataSinkEndpoint {
  readonly topic: string;
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly createTopic: boolean;
  #active = false;
  #partitionCount = 1;
  #binding: KafkaSinkEndpointConsumerContract | undefined;

  public constructor(dataSink: KafkaDataSink, endpointId: number) {
    super(dataSink, endpointId);
    const config = requireKafkaEndpointConfig(this.config());
    this.topic = config.topic;
    this.partitions = config.partitions === 0 ? 1 : config.partitions;
    this.replicationFactor = config.replicationFactor === 0 ? 1 : config.replicationFactor;
    this.createTopic = config.createTopic;
  }

  public active(): boolean {
    return this.#active;
  }

  public enabled(): boolean {
    return requireKafkaEndpointConfig(this.config()).enabled;
  }

  public setActive(active: boolean): void {
    this.#active = active;
  }

  public setPartitionCount(partitionCount: number): void {
    if (!Number.isSafeInteger(partitionCount) || partitionCount < 1) {
      throw new RangeError(`Kafka endpoint ${this.name} has invalid broker partition count`);
    }
    this.#partitionCount = partitionCount;
  }

  public partitionCount(): number {
    return this.#partitionCount;
  }

  public bind(binding: KafkaSinkEndpointConsumerContract): void {
    if (this.#binding !== undefined) {
      throw new Error(`consumer already assigned to Kafka endpoint ${this.name}`);
    }
    this.#binding = binding;
    this.addEndpointConsumer(binding);
  }

  public async start(context: Context): Promise<void> {
    if (!this.enabled()) {
      this.#active = false;
      return;
    }
    await this.#binding?.start(context);
    this.#active = true;
  }

  public async stop(context: Context): Promise<void> {
    this.#active = false;
    await this.#binding?.stop(context);
  }
}

interface KafkaSinkEndpointConsumerContract extends Consumer<unknown> {
  endpoint(): SinkEndpoint;
  start(context: Context): Promise<void>;
  stop(context: Context): Promise<void>;
}

export class KafkaDataSink extends OutputDataSink {
  readonly #factory: KafkaClientFactory;
  readonly #deliveries = new RuntimeTaskRegistry();
  #producer: KafkaProducer | undefined;
  #producerRecovery: Promise<void> | undefined;
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

  public producer(): KafkaProducer {
    if (this.#producer === undefined) {
      throw new Error(`Kafka data sink ${this.name} is not started`);
    }
    return this.#producer;
  }

  public async start(context: Context): Promise<void> {
    void context;
    if (this.#started) {
      throw new Error(`Kafka data sink ${this.name} is already started`);
    }
    this.#started = true;
    const endpoints = this.kafkaEndpoints();
    const enabled = endpoints.filter((endpoint) => endpoint.enabled());
    if (enabled.length === 0) {
      return;
    }
    const config = requireKafkaDataConnectorConfig(this.config());
    const brokers = splitBrokers(config.brokers, this.name);
    const admin = this.#factory.admin(brokers, config.dialTimeout, kafkaSecurity(config));
    try {
      let adminConnected = false;
      try {
        await admin.connect();
        adminConnected = true;
        for (const endpoint of enabled) {
          if (endpoint.topic.length === 0) {
            throw new Error(`no topic specified for Kafka sink endpoint ${endpoint.name}`);
          }
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
    const producer = this.#factory.producer(brokers, config.dialTimeout, kafkaSecurity(config));
    try {
      await producer.connect();
      this.#producer = producer;
      for (const endpoint of enabled) await endpoint.start(context);
    } catch (error: unknown) {
      this.#started = false;
      this.#producer = undefined;
      await Promise.allSettled(enabled.map(async (endpoint) => endpoint.stop(context)));
      await producer.disconnect();
      throw error;
    }
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) return;
    this.#started = false;
    for (const endpoint of this.kafkaEndpoints()) await endpoint.stop(context);
    this.#deliveries.stopAdmission();
    const producer = this.#producer;
    if (producer === undefined) return;
    try {
      await this.#deliveries.drain(context.remainingMs());
      await producer.flush(context.remainingMs());
    } finally {
      this.#producer = undefined;
      await producer.disconnect();
    }
  }

  public send(
    context: MessageContext,
    topic: string,
    key: Uint8Array | undefined,
    value: Uint8Array,
    partition: () => Promise<number | undefined>,
    onDelivery: (result: DeliveryResult | undefined, error: Error | undefined) => Completion
  ): void {
    this.#deliveries.admitDetached(async () => {
      let delivery: DeliveryResult | undefined;
      let failure: Error | undefined;
      let producer: KafkaProducer | undefined;
      try {
        if (context.signal().aborted) {
          throw context.signal().reason === undefined
            ? new Error("Kafka send cancelled")
            : errorFromUnknown(context.signal().reason);
        }
        const selectedPartition = await partition();
        producer = this.#producer;
        if (producer === undefined) throw new Error(`Kafka data sink ${this.name} is stopped`);
        delivery = await producer.send(
          topic,
          key,
          value,
          selectedPartition,
          context.transportMetadata()
        );
      } catch (error: unknown) {
        failure = errorFromUnknown(error);
        if (producer !== undefined) await this.recoverProducer(context, producer);
      }
      await onDelivery(delivery, failure);
    });
  }

  private async recoverProducer(context: Context, failedProducer: KafkaProducer): Promise<void> {
    if (!this.#started || this.#producer !== failedProducer) return;
    if (this.#producerRecovery !== undefined) {
      await this.#producerRecovery;
      return;
    }
    const recovery = this.replaceProducer(failedProducer).catch((error: unknown) => {
      this.runtimeEnvironment()
        .log()
        .error(context, "Kafka producer reconnect failed", err(errorFromUnknown(error)));
    });
    this.#producerRecovery = recovery;
    try {
      await recovery;
    } finally {
      if (this.#producerRecovery === recovery) this.#producerRecovery = undefined;
    }
  }

  private async replaceProducer(failedProducer: KafkaProducer): Promise<void> {
    const config = requireKafkaDataConnectorConfig(this.config());
    const brokers = splitBrokers(config.brokers, this.name);
    const replacement = this.#factory.producer(brokers, config.dialTimeout, kafkaSecurity(config));
    await replacement.connect();
    if (!this.#started || this.#producer !== failedProducer) {
      await replacement.disconnect();
      return;
    }
    this.#producer = replacement;
    await failedProducer.disconnect();
  }

  private kafkaEndpoints(): readonly KafkaSinkEndpoint[] {
    return this.endpoints().map((endpoint) => {
      if (!(endpoint instanceof KafkaSinkEndpoint)) {
        throw new Error(`sink endpoint ${endpoint.name} is not a Kafka endpoint`);
      }
      return endpoint;
    });
  }
}

class KafkaEndpointConsumer<HandlerState, T, R> implements Consumer<T> {
  readonly #base: DataSinkEndpointConsumer<T, R>;
  readonly #stream: TypedSinkStream<T, R>;
  readonly #handler: EndpointHandler<HandlerState, T, R>;
  readonly #partitioner: Partitioner<T> | undefined;
  readonly #tracer: Tracer | undefined;

  public constructor(
    endpoint: KafkaSinkEndpoint,
    stream: TypedSinkStream<T, R>,
    handler: EndpointHandler<HandlerState, T, R>,
    partitioner: Partitioner<T> | undefined
  ) {
    this.#base = new DataSinkEndpointConsumer(endpoint, stream);
    this.#stream = stream;
    this.#handler = handler;
    this.#partitioner = partitioner;
    this.#tracer = stream
      .runtimeEnvironment()
      .tracing()
      ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
  }

  public endpoint(): SinkEndpoint {
    return this.#base.endpoint();
  }

  public start(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public stop(_context: Context): Promise<void> {
    void _context;
    return Promise.resolve();
  }

  public async consume(context: MessageContext, value: T): Promise<void> {
    const endpoint = this.#base.endpoint();
    if (!(endpoint instanceof KafkaSinkEndpoint) || !endpoint.active()) return;
    let span: Span | undefined;
    if (this.#tracer !== undefined && context.samplingEnabled()) {
      const started = this.#tracer.start(context, "kafka.output", [
        stringAttribute("stream", this.#stream.name),
        stringAttribute("endpoint", endpoint.name)
      ]);
      context = started.context;
      span = started.span;
    }
    try {
      await this.consumeTraced(context, value, endpoint, span);
    } finally {
      span?.end();
    }
  }

  private async consumeTraced(
    context: MessageContext,
    value: T,
    endpoint: KafkaSinkEndpoint,
    span: Span | undefined
  ): Promise<void> {
    const streamId = this.#handler.getStreamId(context, value);
    const streamContext = context.withStreamId(streamId);
    span?.setAttributes([stringAttribute("stream_id", streamId)]);
    let state: HandlerState;
    let handlerContext: MessageContext;
    try {
      const started = await this.#handler.beginRequest(streamContext, this.#stream);
      state = started.state;
      handlerContext = started.context;
    } catch (error: unknown) {
      const failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
      endpoint.onBeginRequestFailed(context, failure);
      return;
    }
    span?.addEvent("begin_request");
    const requestStarted = endpoint.onRequestStart(handlerContext);
    let failure: Error | undefined;
    const dataSink = endpoint.dataSink();
    if (!(dataSink instanceof KafkaDataSink)) {
      throw new Error(`Kafka endpoint ${endpoint.name} has an invalid data sink`);
    }
    const message = new SinkMessage<R>(
      endpoint.topic,
      (key, payload, onDelivery) => {
        dataSink.send(
          handlerContext,
          endpoint.topic,
          key,
          payload,
          async () => this.partition(value, endpoint.partitionCount()),
          onDelivery
        );
      },
      (resultContext, result) => this.#stream.errorStream().consume(resultContext, result)
    );
    try {
      await this.#handler.consumeMessage(handlerContext, this.#stream, state, value, message);
      span?.addEvent("consume_message");
    } catch (error: unknown) {
      failure = errorFromUnknown(error);
      spanError(span, failure);
      span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
    } finally {
      try {
        await this.#handler.endRequest(handlerContext, this.#stream, failure, state);
      } catch (error: unknown) {
        failure ??= errorFromUnknown(error);
        spanError(span, failure);
      } finally {
        endpoint.onRequestEnd(handlerContext, requestStarted, failure);
      }
    }
  }

  private async partition(value: T, partitions: number): Promise<number | undefined> {
    if (this.#partitioner === undefined) return Math.floor(Math.random() * partitions);
    const partition = await this.#partitioner.partition(value, partitions);
    if (!Number.isSafeInteger(partition) || partition < 0 || partition >= partitions) {
      throw new RangeError(
        `Kafka partition ${String(partition)} is outside [0, ${String(partitions)})`
      );
    }
    return partition;
  }
}

export function makeKafkaEndpointConsumer<HandlerState, T, R>(
  stream: TypedSinkStream<T, R>,
  handler: EndpointHandler<HandlerState, T, R>,
  factory?: KafkaClientFactory
): Consumer<T> {
  const environment = stream.runtimeEnvironment();
  factory ??= defaultKafkaClientFactory(environment);
  const endpointConfig = requireKafkaEndpointConfig(
    environment.runtimeConfig().endpointById(stream.endpointId())
  );
  const connectorConfig = requireKafkaDataConnectorConfig(
    environment.runtimeConfig().dataConnectorById(endpointConfig.idDataConnector)
  );
  const partitioner = connectorConfig.usePartitioner
    ? requirePartitioner(handler, endpointConfig.name)
    : undefined;
  const dataSink = getOrCreateDataSink(endpointConfig.idDataConnector, environment, factory);
  if (dataSink.endpoint(endpointConfig.id) !== undefined) {
    throw new Error(`endpoint ${endpointConfig.name} already exists`);
  }
  const endpoint = new KafkaSinkEndpoint(dataSink, endpointConfig.id);
  const consumer = new KafkaEndpointConsumer(endpoint, stream, handler, partitioner);
  endpoint.bind(consumer);
  dataSink.addEndpoint(endpoint);
  stream.setSinkConsumer(consumer);
  return consumer;
}

function requirePartitioner<T>(handler: object, endpointName: string): Partitioner<T> {
  if (!("partition" in handler) || typeof handler.partition !== "function") {
    throw new TypeError(`Kafka endpoint ${endpointName} requires its handler to be a partitioner`);
  }
  return handler as Partitioner<T>;
}

function getOrCreateDataSink(
  connectorId: number,
  environment: RuntimeEnvironment,
  factory: KafkaClientFactory
): KafkaDataSink {
  const existing = environment.dataSinkById(connectorId);
  if (existing !== undefined) {
    if (!(existing instanceof KafkaDataSink)) {
      throw new Error(`data sink ${String(connectorId)} is not a Kafka data sink`);
    }
    if (existing.factory() !== factory) {
      throw new Error(`Kafka data sink ${existing.name} already uses a different client factory`);
    }
    return existing;
  }
  const dataSink = new KafkaDataSink(connectorId, environment, factory);
  environment.addDataSink(dataSink);
  return dataSink;
}

function splitBrokers(value: string, connectorName: string): readonly string[] {
  const brokers = value
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
  if (brokers.length === 0) {
    throw new Error(`no brokers specified for Kafka data connector ${connectorName}`);
  }
  return brokers;
}

function makeKafka(
  brokers: readonly string[],
  connectionTimeoutMs: number,
  metrics: Metrics | undefined,
  security?: KafkaSecurity
): KafkaJS.Kafka {
  const statistics =
    metrics === undefined ? undefined : librdkafkaStatisticsOptions(metrics, "producer");
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

class ConfluentProducer implements KafkaProducer {
  readonly #producer: KafkaJS.Producer;

  public constructor(producer: KafkaJS.Producer) {
    this.#producer = producer;
  }

  public connect(): Promise<void> {
    return this.#producer.connect();
  }
  public disconnect(): Promise<void> {
    return this.#producer.disconnect();
  }
  public flush(timeoutMs?: number): Promise<void> {
    return this.#producer.flush(timeoutMs === undefined ? undefined : { timeout: timeoutMs });
  }
  public async send(
    topic: string,
    key: Uint8Array | undefined,
    value: Uint8Array,
    partition?: number,
    headers?: ReadonlyMap<string, string>
  ): Promise<DeliveryResult> {
    const message: KafkaJS.Message = {
      value: Buffer.from(value),
      ...(key === undefined ? {} : { key: Buffer.from(key) }),
      ...(partition === undefined ? {} : { partition }),
      ...(headers === undefined || headers.size === 0
        ? {}
        : { headers: Object.fromEntries(headers) })
    };
    const records = await this.#producer.send({
      topic,
      messages: [message]
    });
    const record = records[0];
    if (record === undefined) throw new Error("Kafka producer returned no delivery metadata");
    return {
      partition: record.partition,
      offset: BigInt(record.offset ?? record.baseOffset ?? "0")
    };
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
