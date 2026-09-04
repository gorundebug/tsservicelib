import { createRequire } from "node:module";
import { DataSinkEndpoint, DataSinkEndpointConsumer, OutputDataSink, RuntimeTaskRegistry, err, errorFromUnknown, requireKafkaDataConnectorConfig, requireKafkaEndpointConfig, spanError, stringAttribute } from "../../runtime/index.js";
import { librdkafkaStatisticsOptions } from "../../runtime/telemetry/librdkafka-statistics.js";
const require = createRequire(import.meta.url);
let confluentKafka;
function kafkaJS() {
    confluentKafka ??=
        require("@confluentinc/kafka-javascript");
    return confluentKafka.KafkaJS;
}
export class ConfluentKafkaClientFactory {
    #metrics;
    constructor(metrics) {
        this.#metrics = metrics;
    }
    producer(brokers, connectionTimeoutMs, security) {
        const kafka = makeKafka(brokers, connectionTimeoutMs, this.#metrics, security);
        return new ConfluentProducer(kafka.producer());
    }
    admin(brokers, connectionTimeoutMs, security) {
        const kafka = makeKafka(brokers, connectionTimeoutMs, undefined, security);
        return new ConfluentAdmin(kafka.admin());
    }
}
const defaultKafkaClientFactories = new WeakMap();
export class SinkMessage {
    key;
    value = new Uint8Array();
    #topic;
    #send;
    #result;
    constructor(topic, send, result) {
        this.#topic = topic;
        this.#send = send;
        this.#result = result;
    }
    topic() {
        return this.#topic;
    }
    send(context, onDelivery) {
        this.#send(this.key, this.value, async (delivery, error) => {
            const result = onDelivery(delivery?.partition ?? 0, delivery?.offset ?? 0n, error);
            if (result !== undefined)
                await this.#result(context, result);
        });
    }
    sendSync(context) {
        return new Promise((resolve, reject) => {
            let settled = false;
            const aborted = () => {
                if (settled)
                    return;
                settled = true;
                reject(context.signal().reason === undefined
                    ? new Error("Kafka send cancelled")
                    : errorFromUnknown(context.signal().reason));
            };
            context.signal().addEventListener("abort", aborted, { once: true });
            try {
                this.#send(this.key, this.value, (delivery, error) => {
                    if (settled)
                        return;
                    settled = true;
                    context.signal().removeEventListener("abort", aborted);
                    if (error !== undefined)
                        reject(error);
                    else if (delivery !== undefined)
                        resolve(delivery);
                    else
                        reject(new Error("Kafka delivery completed without a result"));
                });
            }
            catch (error) {
                settled = true;
                context.signal().removeEventListener("abort", aborted);
                reject(errorFromUnknown(error));
                return;
            }
            if (context.cancelled())
                aborted();
        });
    }
    out(context, value) {
        return this.#result(context, value);
    }
    skip(context, value) {
        return this.#result(context, value);
    }
}
class KafkaSinkEndpoint extends DataSinkEndpoint {
    topic;
    partitions;
    replicationFactor;
    createTopic;
    #active = false;
    #partitionCount = 1;
    #binding;
    constructor(dataSink, endpointId) {
        super(dataSink, endpointId);
        const config = requireKafkaEndpointConfig(this.config());
        this.topic = config.topic;
        this.partitions = config.partitions === 0 ? 1 : config.partitions;
        this.replicationFactor = config.replicationFactor === 0 ? 1 : config.replicationFactor;
        this.createTopic = config.createTopic;
    }
    active() {
        return this.#active;
    }
    enabled() {
        return requireKafkaEndpointConfig(this.config()).enabled;
    }
    setActive(active) {
        this.#active = active;
    }
    setPartitionCount(partitionCount) {
        if (!Number.isSafeInteger(partitionCount) || partitionCount < 1) {
            throw new RangeError(`Kafka endpoint ${this.name} has invalid broker partition count`);
        }
        this.#partitionCount = partitionCount;
    }
    partitionCount() {
        return this.#partitionCount;
    }
    bind(binding) {
        if (this.#binding !== undefined) {
            throw new Error(`consumer already assigned to Kafka endpoint ${this.name}`);
        }
        this.#binding = binding;
        this.addEndpointConsumer(binding);
    }
    async start(context) {
        if (!this.enabled()) {
            this.#active = false;
            return;
        }
        await this.#binding?.start(context);
        this.#active = true;
    }
    async stop(context) {
        this.#active = false;
        await this.#binding?.stop(context);
    }
}
export class KafkaDataSink extends OutputDataSink {
    #factory;
    #deliveries = new RuntimeTaskRegistry();
    #producer;
    #producerRecovery;
    #started = false;
    constructor(connectorId, environment, factory) {
        super(connectorId, environment);
        requireKafkaDataConnectorConfig(this.config());
        this.#factory = factory;
    }
    factory() {
        return this.#factory;
    }
    producer() {
        if (this.#producer === undefined) {
            throw new Error(`Kafka data sink ${this.name} is not started`);
        }
        return this.#producer;
    }
    async start(context) {
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
                        await admin.createTopic(endpoint.topic, endpoint.partitions, endpoint.replicationFactor);
                    }
                    endpoint.setPartitionCount(await admin.partitionCount(endpoint.topic));
                }
            }
            finally {
                if (adminConnected)
                    await admin.disconnect();
            }
        }
        catch (error) {
            this.#started = false;
            throw error;
        }
        const producer = this.#factory.producer(brokers, config.dialTimeout, kafkaSecurity(config));
        try {
            await producer.connect();
            this.#producer = producer;
            for (const endpoint of enabled)
                await endpoint.start(context);
        }
        catch (error) {
            this.#started = false;
            this.#producer = undefined;
            await Promise.allSettled(enabled.map(async (endpoint) => endpoint.stop(context)));
            await producer.disconnect();
            throw error;
        }
    }
    async stop(context) {
        if (!this.#started)
            return;
        this.#started = false;
        for (const endpoint of this.kafkaEndpoints())
            await endpoint.stop(context);
        this.#deliveries.stopAdmission();
        const producer = this.#producer;
        if (producer === undefined)
            return;
        try {
            await this.#deliveries.drain(context.remainingMs());
            await producer.flush(context.remainingMs());
        }
        finally {
            this.#producer = undefined;
            await producer.disconnect();
        }
    }
    send(context, topic, key, value, partition, onDelivery) {
        this.#deliveries.admitDetached(async () => {
            let delivery;
            let failure;
            let producer;
            try {
                if (context.signal().aborted) {
                    throw context.signal().reason === undefined
                        ? new Error("Kafka send cancelled")
                        : errorFromUnknown(context.signal().reason);
                }
                const selectedPartition = await partition();
                producer = this.#producer;
                if (producer === undefined)
                    throw new Error(`Kafka data sink ${this.name} is stopped`);
                delivery = await producer.send(topic, key, value, selectedPartition, context.transportMetadata());
            }
            catch (error) {
                failure = errorFromUnknown(error);
                if (producer !== undefined)
                    await this.recoverProducer(context, producer);
            }
            await onDelivery(delivery, failure);
        });
    }
    async recoverProducer(context, failedProducer) {
        if (!this.#started || this.#producer !== failedProducer)
            return;
        if (this.#producerRecovery !== undefined) {
            await this.#producerRecovery;
            return;
        }
        const recovery = this.replaceProducer(failedProducer).catch((error) => {
            this.runtimeEnvironment()
                .log()
                .error(context, "Kafka producer reconnect failed", err(errorFromUnknown(error)));
        });
        this.#producerRecovery = recovery;
        try {
            await recovery;
        }
        finally {
            if (this.#producerRecovery === recovery)
                this.#producerRecovery = undefined;
        }
    }
    async replaceProducer(failedProducer) {
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
    kafkaEndpoints() {
        return this.endpoints().map((endpoint) => {
            if (!(endpoint instanceof KafkaSinkEndpoint)) {
                throw new Error(`sink endpoint ${endpoint.name} is not a Kafka endpoint`);
            }
            return endpoint;
        });
    }
}
class KafkaEndpointConsumer {
    #base;
    #stream;
    #handler;
    #partitioner;
    #tracer;
    constructor(endpoint, stream, handler, partitioner) {
        this.#base = new DataSinkEndpointConsumer(endpoint, stream);
        this.#stream = stream;
        this.#handler = handler;
        this.#partitioner = partitioner;
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    endpoint() {
        return this.#base.endpoint();
    }
    start(_context) {
        void _context;
        return Promise.resolve();
    }
    stop(_context) {
        void _context;
        return Promise.resolve();
    }
    async consume(context, value) {
        const endpoint = this.#base.endpoint();
        if (!(endpoint instanceof KafkaSinkEndpoint) || !endpoint.active())
            return;
        let span;
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
        }
        finally {
            span?.end();
        }
    }
    async consumeTraced(context, value, endpoint, span) {
        const streamId = this.#handler.getStreamId(context, value);
        const streamContext = context.withStreamId(streamId);
        span?.setAttributes([stringAttribute("stream_id", streamId)]);
        let state;
        let handlerContext;
        try {
            const started = await this.#handler.beginRequest(streamContext, this.#stream);
            state = started.state;
            handlerContext = started.context;
        }
        catch (error) {
            const failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("begin_request.error", [stringAttribute("error", failure.message)]);
            endpoint.onBeginRequestFailed(context, failure);
            return;
        }
        span?.addEvent("begin_request");
        const requestStarted = endpoint.onRequestStart(handlerContext);
        let failure;
        const dataSink = endpoint.dataSink();
        if (!(dataSink instanceof KafkaDataSink)) {
            throw new Error(`Kafka endpoint ${endpoint.name} has an invalid data sink`);
        }
        const message = new SinkMessage(endpoint.topic, (key, payload, onDelivery) => {
            dataSink.send(handlerContext, endpoint.topic, key, payload, async () => this.partition(value, endpoint.partitionCount()), onDelivery);
        }, (resultContext, result) => this.#stream.errorStream().consume(resultContext, result));
        try {
            await this.#handler.consumeMessage(handlerContext, this.#stream, state, value, message);
            span?.addEvent("consume_message");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            spanError(span, failure);
            span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
        }
        finally {
            try {
                await this.#handler.endRequest(handlerContext, this.#stream, failure, state);
            }
            catch (error) {
                failure ??= errorFromUnknown(error);
                spanError(span, failure);
            }
            finally {
                endpoint.onRequestEnd(handlerContext, requestStarted, failure);
            }
        }
    }
    async partition(value, partitions) {
        if (this.#partitioner === undefined)
            return Math.floor(Math.random() * partitions);
        const partition = await this.#partitioner.partition(value, partitions);
        if (!Number.isSafeInteger(partition) || partition < 0 || partition >= partitions) {
            throw new RangeError(`Kafka partition ${String(partition)} is outside [0, ${String(partitions)})`);
        }
        return partition;
    }
}
export function makeKafkaEndpointConsumer(stream, handler, factory) {
    const environment = stream.runtimeEnvironment();
    factory ??= defaultKafkaClientFactory(environment);
    const endpointConfig = requireKafkaEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const connectorConfig = requireKafkaDataConnectorConfig(environment.runtimeConfig().dataConnectorById(endpointConfig.idDataConnector));
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
function requirePartitioner(handler, endpointName) {
    if (!("partition" in handler) || typeof handler.partition !== "function") {
        throw new TypeError(`Kafka endpoint ${endpointName} requires its handler to be a partitioner`);
    }
    return handler;
}
function getOrCreateDataSink(connectorId, environment, factory) {
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
function splitBrokers(value, connectorName) {
    const brokers = value
        .split(",")
        .map((broker) => broker.trim())
        .filter(Boolean);
    if (brokers.length === 0) {
        throw new Error(`no brokers specified for Kafka data connector ${connectorName}`);
    }
    return brokers;
}
function makeKafka(brokers, connectionTimeoutMs, metrics, security) {
    const statistics = metrics === undefined ? undefined : librdkafkaStatisticsOptions(metrics, "producer");
    return new (kafkaJS().Kafka)({
        ...statistics,
        kafkaJS: {
            brokers: [...brokers],
            ...kafkaSecurityOptions(security),
            ...(connectionTimeoutMs === 0 ? {} : { connectionTimeout: connectionTimeoutMs })
        }
    });
}
function kafkaSecurity(config) {
    return {
        protocol: config.securityProtocol,
        mechanism: config.saslMechanism,
        username: config.username,
        password: config.password
    };
}
function kafkaSecurityOptions(security) {
    if (security === undefined || security.protocol === "PLAINTEXT")
        return {};
    if (security.username === undefined ||
        security.username === "" ||
        security.password === undefined ||
        security.password === "") {
        throw new Error("Kafka SASL username and password must both be configured");
    }
    return {
        ssl: security.protocol === "SASL_SSL",
        sasl: {
            mechanism: security.mechanism.toLowerCase(),
            username: security.username,
            password: security.password
        }
    };
}
function defaultKafkaClientFactory(environment) {
    const existing = defaultKafkaClientFactories.get(environment);
    if (existing !== undefined)
        return existing;
    const factory = new ConfluentKafkaClientFactory(environment.metrics());
    defaultKafkaClientFactories.set(environment, factory);
    return factory;
}
class ConfluentProducer {
    #producer;
    constructor(producer) {
        this.#producer = producer;
    }
    connect() {
        return this.#producer.connect();
    }
    disconnect() {
        return this.#producer.disconnect();
    }
    flush(timeoutMs) {
        return this.#producer.flush(timeoutMs === undefined ? undefined : { timeout: timeoutMs });
    }
    async send(topic, key, value, partition, headers) {
        const message = {
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
        if (record === undefined)
            throw new Error("Kafka producer returned no delivery metadata");
        return {
            partition: record.partition,
            offset: BigInt(record.offset ?? record.baseOffset ?? "0")
        };
    }
}
class ConfluentAdmin {
    #admin;
    constructor(admin) {
        this.#admin = admin;
    }
    connect() {
        return this.#admin.connect();
    }
    disconnect() {
        return this.#admin.disconnect();
    }
    async createTopic(topic, partitions, replicationFactor) {
        await this.#admin.createTopics({
            topics: [{ topic, numPartitions: partitions, replicationFactor }]
        });
    }
    async partitionCount(topic) {
        const metadata = await this.#admin.fetchTopicMetadata({ topics: [topic] });
        const topicMetadata = metadataTopics(metadata).find((candidate) => candidate.name === topic);
        if (topicMetadata === undefined)
            throw new Error(`Kafka topic ${topic} metadata was not found`);
        return topicMetadata.partitions.length;
    }
}
function metadataTopics(value) {
    let rawTopics;
    if (Array.isArray(value))
        rawTopics = value;
    else if (typeof value === "object" && value !== null && "topics" in value) {
        rawTopics = value.topics;
    }
    if (!Array.isArray(rawTopics))
        throw new TypeError("Kafka topic metadata has an invalid shape");
    const topics = rawTopics;
    return topics.map((topic) => {
        if (typeof topic !== "object" ||
            topic === null ||
            !("name" in topic) ||
            typeof topic.name !== "string" ||
            !("partitions" in topic) ||
            !Array.isArray(topic.partitions)) {
            throw new TypeError("Kafka topic metadata entry has an invalid shape");
        }
        return { name: topic.name, partitions: topic.partitions };
    });
}
//# sourceMappingURL=confluent.js.map