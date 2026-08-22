import { KafkaJS } from "@confluentinc/kafka-javascript";
import { DataSourceEndpoint, DataSourceEndpointConsumer, FunctionCollector, InputDataSource, Context, MessageContext, RotatingMap, RuntimeTaskRegistry, boolAttribute, err, errorFromUnknown, makeStreamContext, newStreamId, requireKafkaDataConnectorConfig, requireKafkaEndpointConfig, spanError, stringAttribute } from "../../runtime/index.js";
import { librdkafkaStatisticsOptions } from "../../runtime/telemetry/librdkafka-statistics.js";
const PENDING_ROTATION_INTERVAL_MS = 30_000;
const RECONNECT_DELAY_MS = 100;
export class ConsumerMessage {
    key;
    value;
    topic;
    partition;
    offset;
    #record;
    #control;
    constructor(record, control) {
        this.#record = record;
        this.#control = control;
        this.key = record.key;
        this.value = record.value;
        this.topic = record.topic;
        this.partition = record.partition;
        this.offset = record.offset;
    }
    markMessage(metadata = "") {
        this.#control.mark(this.#record, metadata);
    }
    commit() {
        return this.#control.commit(this.#record);
    }
}
export class ConfluentKafkaClientFactory {
    #metrics;
    constructor(metrics) {
        this.#metrics = metrics;
    }
    consumer(brokers, groupId, connectionTimeoutMs, security) {
        return new ConfluentConsumer(makeKafka(brokers, connectionTimeoutMs, this.#metrics, security).consumer({
            kafkaJS: { groupId, fromBeginning: true, autoCommit: true }
        }));
    }
    admin(brokers, connectionTimeoutMs, security) {
        return new ConfluentAdmin(makeKafka(brokers, connectionTimeoutMs, undefined, security).admin());
    }
}
const defaultKafkaClientFactories = new WeakMap();
class KafkaResult {
    state;
    #span;
    #recordDone;
    #callbacks = new Map();
    #done;
    #resolveDone;
    #completed = false;
    #retiring = false;
    #activeCallbacks = 0;
    #retired;
    #resolveRetired;
    constructor(state, span, recordDone) {
        this.state = state;
        this.#span = span;
        this.#recordDone = recordDone;
        this.#done = new Promise((resolve) => {
            this.#resolveDone = resolve;
        });
    }
    setResultCallback(messageId, callback) {
        this.#callbacks.set(messageId, callback);
    }
    callback(messageId) {
        return this.#callbacks.get(messageId);
    }
    remove(messageId, callback) {
        if (this.#callbacks.get(messageId) !== callback)
            return false;
        return this.#callbacks.delete(messageId);
    }
    done() {
        if (this.#completed)
            return;
        this.#completed = true;
        if (this.#recordDone)
            this.#span?.addEvent("done_called");
        this.#resolveDone?.();
        this.#resolveDone = undefined;
    }
    wait() {
        return this.#done;
    }
    span() {
        return this.#span;
    }
    beginCallback() {
        if (this.#retiring)
            return false;
        this.#activeCallbacks += 1;
        return true;
    }
    endCallback() {
        this.#activeCallbacks -= 1;
        if (this.#retiring && this.#activeCallbacks === 0) {
            this.#resolveRetired?.();
            this.#resolveRetired = undefined;
        }
    }
    async retire() {
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
    topic;
    partitions;
    replicationFactor;
    createTopic;
    consumerGroup;
    #partitionCount = 1;
    #consumer;
    #binding;
    #run;
    #reconnect;
    constructor(dataSource, endpointId) {
        super(dataSource, endpointId);
        const config = requireKafkaEndpointConfig(this.config());
        this.topic = config.topic;
        this.partitions = config.partitions === 0 ? 1 : config.partitions;
        this.replicationFactor = config.replicationFactor === 0 ? 1 : config.replicationFactor;
        this.createTopic = config.createTopic;
        this.consumerGroup = config.consumerGroup;
    }
    bind(binding) {
        if (this.#binding !== undefined)
            throw new Error(`consumer already assigned to Kafka endpoint ${this.name}`);
        this.#binding = binding;
        this.addEndpointConsumer(binding);
    }
    enabled() {
        return requireKafkaEndpointConfig(this.config()).enabled;
    }
    validate() {
        if (this.topic.length === 0)
            throw new Error(`no topic specified for Kafka endpoint ${this.name}`);
        if (this.consumerGroup.length === 0)
            throw new Error(`no consumer group specified for Kafka endpoint ${this.name}`);
    }
    setPartitionCount(partitionCount) {
        if (!Number.isSafeInteger(partitionCount) || partitionCount < 1) {
            throw new RangeError(`Kafka endpoint ${this.name} has invalid broker partition count`);
        }
        this.#partitionCount = partitionCount;
    }
    async start(context) {
        if (!this.enabled())
            return;
        this.validate();
        const reconnect = new AbortController();
        const consumer = await this.connectCurrentConsumer();
        try {
            this.#consumer = consumer;
            this.#reconnect = reconnect;
            await this.#binding?.start();
            this.#run = this.supervise(context, consumer, reconnect.signal);
        }
        catch (error) {
            this.#consumer = undefined;
            this.#reconnect = undefined;
            await consumer.disconnect();
            throw error;
        }
    }
    async stop(context) {
        this.#reconnect?.abort(new Error(`Kafka endpoint ${this.name} stopped`));
        this.#reconnect = undefined;
        await this.#binding?.stop(context);
        const consumer = this.#consumer;
        if (consumer !== undefined) {
            await consumer.stop();
        }
        await this.#run;
        this.#consumer = undefined;
        this.#run = undefined;
    }
    async supervise(context, initial, signal) {
        let consumer = initial;
        for (;;) {
            if (consumer === undefined) {
                if (!(await reconnectDelay(signal)))
                    return;
                try {
                    consumer = await this.connectCurrentConsumer();
                    if (signal.aborted) {
                        await consumer.disconnect();
                        return;
                    }
                    this.#consumer = consumer;
                }
                catch (error) {
                    this.logReconnectFailure(context, error);
                    continue;
                }
            }
            try {
                await consumer.run(this.#partitionCount, async (record, control) => this.#binding?.handle(record, control));
                if (!signal.aborted) {
                    this.logReconnectFailure(context, new Error("Kafka consumer stopped unexpectedly"));
                }
            }
            catch (error) {
                if (!signal.aborted)
                    this.logReconnectFailure(context, error);
            }
            finally {
                if (this.#consumer === consumer)
                    this.#consumer = undefined;
                await consumer.disconnect();
                consumer = undefined;
            }
            if (signal.aborted)
                return;
        }
    }
    async connectCurrentConsumer() {
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
        }
        catch (error) {
            await consumer.disconnect();
            throw error;
        }
    }
    logReconnectFailure(context, error) {
        this.runtimeEnvironment()
            .log()
            .error(context, "Kafka consumer reconnect required", err(errorFromUnknown(error)));
    }
}
export class KafkaDataSource extends InputDataSource {
    #factory;
    #started = false;
    constructor(connectorId, environment, factory) {
        super(connectorId, environment);
        requireKafkaDataConnectorConfig(this.config());
        this.#factory = factory;
    }
    factory() {
        return this.#factory;
    }
    async start(context) {
        void context;
        if (this.#started)
            throw new Error(`Kafka data source ${this.name} is already started`);
        this.#started = true;
        const endpoints = this.kafkaEndpoints();
        const enabled = endpoints.filter((endpoint) => endpoint.enabled());
        if (enabled.length === 0)
            return;
        const config = requireKafkaDataConnectorConfig(this.config());
        const brokers = splitBrokers(config.brokers, this.name);
        const admin = this.#factory.admin(brokers, config.dialTimeout, kafkaSecurity(config));
        try {
            let adminConnected = false;
            try {
                for (const endpoint of enabled)
                    endpoint.validate();
                await admin.connect();
                adminConnected = true;
                for (const endpoint of enabled) {
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
        try {
            for (const endpoint of enabled)
                await endpoint.start(context);
        }
        catch (error) {
            this.#started = false;
            await Promise.allSettled(enabled.map(async (endpoint) => endpoint.stop(Context.background())));
            throw error;
        }
    }
    async stop(context) {
        if (!this.#started)
            return;
        this.#started = false;
        await Promise.all(this.kafkaEndpoints().map(async (endpoint) => endpoint.stop(context)));
    }
    kafkaEndpoints() {
        return this.endpoints().map((endpoint) => {
            if (!(endpoint instanceof KafkaSourceEndpoint))
                throw new Error(`source endpoint ${endpoint.name} is not Kafka`);
            return endpoint;
        });
    }
}
class KafkaEndpointConsumer extends DataSourceEndpointConsumer {
    #streamContext;
    #handler;
    #tasks = new RuntimeTaskRegistry();
    #pending;
    #waiters = [];
    #tracer;
    #active = 0;
    #started = false;
    #stopped = false;
    constructor(endpoint, stream, handler) {
        super(endpoint, stream);
        this.#handler = handler;
        this.#streamContext = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((context, value) => stream.consume(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        stream.setResultConsumer({
            consume: (context, value) => this.consumeResult(context, value)
        });
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    start() {
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
    async stop(context) {
        if (!this.#started)
            return;
        this.#started = false;
        this.#stopped = true;
        for (const wake of this.#waiters.splice(0))
            wake();
        this.#tasks.cancel(context.signal().reason ?? new Error("Kafka endpoint stopped"));
        try {
            await this.#tasks.drain(context.remainingMs());
        }
        finally {
            this.#pending?.stop(context);
        }
    }
    consume(context, value) {
        void context;
        void value;
    }
    handle(record, control) {
        if (!this.#started)
            return Promise.resolve();
        return this.#tasks.admit(async (signal) => this.handleOnce(record, control, signal));
    }
    async handleOnce(record, control, signal) {
        await this.acquire(signal);
        try {
            await this.handleAdmitted(record, control, signal);
        }
        finally {
            this.#active -= 1;
            this.#waiters.shift()?.();
        }
    }
    async handleAdmitted(record, control, signal) {
        let context = new MessageContext().withExternalCancellation(signal);
        let span;
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
        }
        finally {
            span?.end();
        }
    }
    async handleTraced(record, control, context, span) {
        let state;
        try {
            const started = await this.#handler.beginRequest(context, this.#streamContext);
            context = started.context;
            state = started.state;
        }
        catch (error) {
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
        const result = new KafkaResult(state, span, hasResult);
        if (hasResult) {
            try {
                this.pending().set(streamId, result);
                this.endpoint().onPendingAdd(context, streamId);
            }
            catch (error) {
                const failure = errorFromUnknown(error);
                spanError(span, failure);
                await this.#handler.endRequest(context, this.#streamContext, failure, state);
                this.endpoint().onRequestEnd(context, startedAt, failure);
                return;
            }
        }
        let failure;
        let resultWaitFailed = false;
        try {
            await this.#handler.consumeMessage(context, this.#streamContext, state, new ConsumerMessage(record, control), result);
            span?.addEvent("consume_message");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
        }
        if (failure === undefined) {
            if (!hasResult)
                result.done();
            try {
                await waitForResult(result, context.signal());
                if (hasResult)
                    span?.addEvent("done_received");
            }
            catch (error) {
                failure = errorFromUnknown(error);
                resultWaitFailed = true;
            }
        }
        if (hasResult) {
            const resultCompleted = await result.retire();
            if (resultWaitFailed && resultCompleted)
                failure = undefined;
            this.pending().pop(streamId);
            this.endpoint().onPendingRemove(context, streamId);
        }
        if (failure !== undefined)
            spanError(span, failure);
        try {
            await this.#handler.endRequest(context, this.#streamContext, failure, state);
        }
        catch (error) {
            failure ??= errorFromUnknown(error);
            spanError(span, failure);
        }
        finally {
            this.endpoint().onRequestEnd(context, startedAt, failure);
        }
    }
    async acquire(signal) {
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
            await new Promise((resolve) => this.#waiters.push(resolve));
        }
    }
    async consumeResult(context, value) {
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
            const messageId = this.#handler.getMessageId(context, this.#streamContext, result.state, value);
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
        }
        finally {
            result.endCallback();
        }
    }
    pending() {
        if (this.#pending === undefined) {
            throw new Error(`Kafka endpoint ${this.endpoint().name} pending store is not started`);
        }
        return this.#pending;
    }
}
export function makeKafkaEndpointConsumer(stream, handler, factory) {
    const environment = stream.runtimeEnvironment();
    factory ??= defaultKafkaClientFactory(environment);
    const endpointConfig = requireKafkaEndpointConfig(environment.runtimeConfig().endpointById(stream.endpointId()));
    const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment, factory);
    if (dataSource.endpoint(endpointConfig.id) !== undefined)
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    const endpoint = new KafkaSourceEndpoint(dataSource, endpointConfig.id);
    const consumer = new KafkaEndpointConsumer(endpoint, stream, handler);
    endpoint.bind(consumer);
    dataSource.addEndpoint(endpoint);
    return consumer;
}
function getOrCreateDataSource(connectorId, environment, factory) {
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
function splitBrokers(value, connectorName) {
    const brokers = value
        .split(",")
        .map((broker) => broker.trim())
        .filter(Boolean);
    if (brokers.length === 0)
        throw new Error(`no brokers specified for Kafka data connector ${connectorName}`);
    return brokers;
}
async function reconnectDelay(signal) {
    if (signal.aborted)
        return false;
    return await new Promise((resolve) => {
        const timer = setTimeout(() => {
            signal.removeEventListener("abort", cancel);
            resolve(true);
        }, RECONNECT_DELAY_MS);
        const cancel = () => {
            clearTimeout(timer);
            resolve(false);
        };
        signal.addEventListener("abort", cancel, { once: true });
    });
}
async function waitForResult(result, signal) {
    if (signal.aborted) {
        throw signal.reason === undefined
            ? new Error("Kafka request cancelled")
            : errorFromUnknown(signal.reason);
    }
    let cancelled;
    try {
        await Promise.race([
            result.wait(),
            new Promise((_resolve, reject) => {
                cancelled = () => {
                    reject(signal.reason === undefined
                        ? new Error("Kafka request cancelled")
                        : errorFromUnknown(signal.reason));
                };
                signal.addEventListener("abort", cancelled, { once: true });
            })
        ]);
    }
    finally {
        if (cancelled !== undefined)
            signal.removeEventListener("abort", cancelled);
    }
}
function makeKafka(brokers, connectionTimeoutMs, metrics, security) {
    const statistics = metrics === undefined ? undefined : librdkafkaStatisticsOptions(metrics, "consumer");
    return new KafkaJS.Kafka({
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
class ConfluentConsumer {
    #consumer;
    #finishRun;
    constructor(consumer) {
        this.#consumer = consumer;
    }
    connect() {
        return this.#consumer.connect();
    }
    async disconnect() {
        try {
            await this.#consumer.disconnect();
        }
        finally {
            this.finishRun();
        }
    }
    subscribe(topic) {
        return this.#consumer.subscribe({ topic });
    }
    async stop() {
        try {
            await this.#consumer.stop();
        }
        finally {
            this.finishRun();
        }
    }
    async run(concurrency, handler) {
        if (this.#finishRun !== undefined)
            throw new Error("Kafka consumer is already running");
        let finish;
        const lifetime = new Promise((resolve) => {
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
                        if (!payload.isRunning() || payload.isStale())
                            break;
                        const record = kafkaRecord(batch.topic, batch.partition, message);
                        const control = new ConfluentMessageControl(this.#consumer, record, (offset) => {
                            payload.resolveOffset(offset);
                        }, async () => payload.commitOffsetsIfNecessary());
                        await handler(record, control);
                        await control.complete();
                        await payload.heartbeat();
                    }
                }
            });
            await lifetime;
        }
        finally {
            if (this.#finishRun === finish)
                this.#finishRun = undefined;
        }
    }
    finishRun() {
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
class ConfluentMessageControl {
    #consumer;
    #record;
    #resolveOffset;
    #commitOffsetsIfNecessary;
    #marked = false;
    #committed = false;
    #metadata = "";
    constructor(consumer, record, resolveOffset, commitOffsetsIfNecessary) {
        this.#consumer = consumer;
        this.#record = record;
        this.#resolveOffset = resolveOffset;
        this.#commitOffsetsIfNecessary = commitOffsetsIfNecessary;
    }
    mark(record, metadata) {
        this.requireRecord(record);
        this.#marked = true;
        this.#metadata = metadata;
    }
    async commit(record) {
        this.requireRecord(record);
        await this.#consumer.commitOffsets([nextOffset(record)]);
        this.#committed = true;
    }
    async complete() {
        if (this.#committed)
            return;
        if (!this.#marked)
            return;
        if (this.#metadata.length > 0) {
            await this.#consumer.commitOffsets([nextOffset(this.#record, this.#metadata)]);
            return;
        }
        this.#resolveOffset(String(this.#record.offset));
        await this.#commitOffsetsIfNecessary();
    }
    requireRecord(record) {
        if (record !== this.#record) {
            throw new Error("Kafka consumer control belongs to another message");
        }
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
function kafkaRecord(topic, partition, message) {
    const headers = new Map();
    for (const [name, raw] of Object.entries(message.headers ?? {})) {
        const first = Array.isArray(raw) ? raw[0] : raw;
        if (first !== undefined)
            headers.set(name, Buffer.from(first));
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
function nextOffset(record, metadata) {
    return {
        topic: record.topic,
        partition: record.partition,
        offset: String(record.offset + 1n),
        ...(metadata === undefined ? {} : { metadata })
    };
}
//# sourceMappingURL=confluent.js.map