import { createRequire } from "node:module";
import { makeEndpointTraceAttributes } from "../../runtime/endpoint-tracing.js";
import { applyDataSourceEndpointTracing, ContextDeadlineExceededError, DataSourceEndpoint, DataSourceEndpointConsumer, FunctionCollector, InputDataSource, MessageContext, RotatingMap, RuntimeDrainTimeoutError, RuntimeTaskRegistry, TRACE_SAMPLING_HEADER, boolAttribute, err, errorFromUnknown, makeStreamContext, newStreamId, requireKafkaDataConnectorConfig, requireKafkaEndpointConfig, settleWithinDeadline, spanError, stringAttribute } from "../../runtime/index.js";
import { librdkafkaStatisticsOptions } from "../../runtime/telemetry/librdkafka-statistics.js";
const PENDING_ROTATION_INTERVAL_MS = 30_000;
const RECONNECT_DELAY_MS = 100;
const require = createRequire(import.meta.url);
let confluentKafka;
function kafkaJS() {
    confluentKafka ??= require("@confluentinc/kafka-javascript");
    return confluentKafka.KafkaJS;
}
export class KafkaCheckpointError extends Error {
}
function validateCheckpoint(offsets) {
    if (offsets === null)
        return;
    for (const [partition, next] of offsets) {
        if (!Number.isSafeInteger(partition) || partition < 0 || partition > 0x7fffffff ||
            typeof next !== "bigint" || next < 0n || next > BigInt(Number.MAX_SAFE_INTEGER)) {
            throw new KafkaCheckpointError("Kafka checkpoint partition/next offset is not exactly representable by the native JavaScript client");
        }
    }
}
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
    consumer(brokers, groupId, connectionTimeoutMs, checkpoint, security) {
        confluentKafka ??= require("@confluentinc/kafka-javascript");
        return new ConfluentConsumer(confluentKafka, brokers, groupId, connectionTimeoutMs, checkpoint, this.#metrics, security);
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
    #restoring = false;
    #consumer;
    #binding;
    #run;
    #reconnect;
    #admissionStopped = false;
    #connecting = false;
    #disconnecting;
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
    async start(context, checkActive = () => { assertKafkaStartupContext(context); }) {
        if (!this.enabled())
            return;
        this.validate();
        checkActive();
        if (this.#run !== undefined)
            throw new Error(`Kafka endpoint ${this.name} already started`);
        const reconnect = new AbortController();
        const owner = context.withExternalCancellation(reconnect.signal);
        this.#reconnect = reconnect;
        this.#admissionStopped = false;
        await this.#binding?.start(owner);
        this.#run = this.startConsumer(owner, () => {
            checkActive();
            assertKafkaStartupContext(owner);
        }, reconnect.signal);
    }
    async startConsumer(context, checkActive, signal) {
        try {
            const initial = await this.#binding.restoreCheckpoint(context);
            checkActive();
            validateCheckpoint(initial);
            this.#restoring = initial !== null;
            const consumer = await this.connectCurrentConsumer(context, checkActive);
            checkActive();
            await this.supervise(context, consumer, signal);
        }
        catch (error) {
            if (!signal.aborted && !context.cancelled())
                await this.logReconnectFailure(context, error);
        }
    }
    async stop(context) {
        await this.stopAdmission(context);
        await this.#binding?.stop(context);
        if (this.#run !== undefined)
            await waitForKafkaShutdown(context, this.#run);
        if (this.#consumer !== undefined) {
            await waitForKafkaShutdown(context, this.disconnectConsumer(this.#consumer));
        }
        this.#run = undefined;
    }
    async stopAdmission(context) {
        if (!this.#admissionStopped) {
            this.#admissionStopped = true;
            this.#reconnect?.abort(new Error(`Kafka endpoint ${this.name} stopped`));
            this.#reconnect = undefined;
            this.#binding?.stopAdmission();
        }
        const consumer = this.#consumer;
        if (consumer !== undefined && !this.#connecting && this.#run !== undefined) {
            await waitForKafkaShutdown(context, consumer.stop(context));
        }
    }
    async supervise(context, initial, signal) {
        let consumer = initial;
        for (;;) {
            if (consumer === undefined) {
                if (!(await reconnectDelay(signal)))
                    return;
                try {
                    consumer = await this.connectCurrentConsumer(context, () => {
                        assertKafkaStartupContext(context);
                        if (signal.aborted)
                            throw errorFromUnknown(signal.reason);
                    });
                    if (signal.aborted) {
                        await this.disconnectConsumer(consumer);
                        return;
                    }
                    this.#consumer = consumer;
                }
                catch (error) {
                    if (signal.aborted || context.cancelled())
                        return;
                    await this.logReconnectFailure(context, error);
                    if (this.#consumer !== undefined || error instanceof KafkaCheckpointError)
                        return;
                    continue;
                }
            }
            let cleanupFailed = false;
            let checkpointFailed = false;
            try {
                await consumer.run(this.#partitionCount, async (record, control) => this.#binding?.handle(context, record, control));
                if (!signal.aborted) {
                    await this.logReconnectFailure(context, new Error("Kafka consumer stopped unexpectedly"));
                }
            }
            catch (error) {
                checkpointFailed = error instanceof KafkaCheckpointError;
                if (!signal.aborted)
                    await this.logReconnectFailure(context, error);
            }
            finally {
                try {
                    await this.disconnectConsumer(consumer);
                }
                catch (error) {
                    // Keep the client for a later stop retry instead of losing ownership.
                    await this.logReconnectFailure(context, error);
                    cleanupFailed = true;
                }
                consumer = undefined;
            }
            if (cleanupFailed || checkpointFailed)
                return;
            if (signal.aborted)
                return;
        }
    }
    async connectCurrentConsumer(context, checkActive) {
        checkActive();
        const dataSource = this.dataSource();
        if (!(dataSource instanceof KafkaDataSource)) {
            throw new Error(`invalid Kafka data source for ${this.name}`);
        }
        const config = requireKafkaDataConnectorConfig(dataSource.config());
        const brokers = splitBrokers(config.brokers, dataSource.name);
        const consumer = dataSource
            .factory()
            .consumer(brokers, this.consumerGroup, config.dialTimeout, {
            context, restoring: this.#restoring,
            restore: () => this.#binding.restoreCheckpoint(context)
        }, kafkaSecurity(config));
        this.#consumer = consumer;
        this.#connecting = true;
        try {
            await consumer.connect();
            checkActive();
            await consumer.subscribe(this.topic);
            checkActive();
            return consumer;
        }
        catch (error) {
            try {
                await this.disconnectConsumer(consumer);
            }
            catch (cleanupError) {
                await this.logReconnectFailure(context, cleanupError);
            }
            throw error;
        }
        finally {
            this.#connecting = false;
        }
    }
    disconnectConsumer(consumer) {
        if (this.#disconnecting !== undefined)
            return this.#disconnecting;
        const operation = consumer.disconnect().then(() => {
            if (this.#consumer === consumer)
                this.#consumer = undefined;
        });
        this.#disconnecting = operation;
        const settled = () => { this.#disconnecting = undefined; };
        void operation.then(settled, settled);
        return operation;
    }
    async logReconnectFailure(context, error) {
        this.runtimeEnvironment()
            .log()
            .error(context, error instanceof KafkaCheckpointError ? "Kafka checkpoint recovery failed" : "Kafka consumer reconnect required", err(errorFromUnknown(error)));
        if (!context.signal().aborted) {
            try {
                await this.#binding?.onError(context, errorFromUnknown(error));
            }
            catch (callbackError) {
                this.runtimeEnvironment().log().error(context, "Kafka onError failed", err(errorFromUnknown(callbackError)));
            }
        }
    }
}
export class KafkaDataSource extends InputDataSource {
    #factory;
    #started = false;
    #starting;
    #stopping = false;
    #admin;
    #adminClosing;
    constructor(connectorId, environment, factory) {
        super(connectorId, environment);
        requireKafkaDataConnectorConfig(this.config());
        this.#factory = factory;
    }
    factory() {
        return this.#factory;
    }
    async start(context) {
        if (this.#started)
            throw new Error(`Kafka data source ${this.name} is already started`);
        assertKafkaStartupContext(context);
        this.#started = true;
        this.#stopping = false;
        // Native connect has no cancellation API. Retain its task and clients until
        // it settles; cancelling the caller must not permit a second startup.
        const operation = this.startOwned(context);
        this.#starting = operation;
        const settled = () => { this.#starting = undefined; };
        void operation.then(settled, settled);
        await waitForKafkaStartup(context, operation);
    }
    async startOwned(context) {
        const enabled = [];
        const checkActive = () => {
            assertKafkaStartupContext(context);
            if (this.#stopping)
                throw new Error(`Kafka data source ${this.name} stopped during startup`);
        };
        try {
            enabled.push(...this.kafkaEndpoints().filter((endpoint) => endpoint.enabled()));
            if (enabled.length === 0)
                return;
            for (const endpoint of enabled)
                endpoint.validate();
            checkActive();
            const config = requireKafkaDataConnectorConfig(this.config());
            const brokers = splitBrokers(config.brokers, this.name);
            const admin = this.#factory.admin(brokers, config.dialTimeout, kafkaSecurity(config));
            this.#admin = admin;
            await admin.connect();
            checkActive();
            for (const endpoint of enabled) {
                if (endpoint.createTopic) {
                    await admin.createTopic(endpoint.topic, endpoint.partitions, endpoint.replicationFactor);
                    checkActive();
                }
                endpoint.setPartitionCount(await admin.partitionCount(endpoint.topic));
                checkActive();
            }
            await this.closeAdmin();
            for (const endpoint of enabled) {
                checkActive();
                await endpoint.start(context, checkActive);
            }
        }
        catch (error) {
            const cleanup = await Promise.allSettled([
                this.closeAdmin(),
                ...enabled.map(async (endpoint) => endpoint.stop(context))
            ]);
            this.#started = cleanup.some((result) => result.status === "rejected");
            throw error;
        }
    }
    closeAdmin() {
        if (this.#adminClosing !== undefined)
            return this.#adminClosing;
        const admin = this.#admin;
        if (admin === undefined)
            return Promise.resolve();
        const operation = admin.disconnect().then(() => { this.#admin = undefined; });
        this.#adminClosing = operation;
        const settled = () => { this.#adminClosing = undefined; };
        void operation.then(settled, settled);
        return operation;
    }
    async joinStartup(context) {
        this.#stopping = true;
        if (this.#starting !== undefined) {
            // Startup errors belong to start(); shutdown retries cleanup separately.
            await waitForKafkaShutdown(context, this.#starting.then(() => undefined, () => undefined));
        }
    }
    async stop(context) {
        if (!this.#started)
            return;
        await this.joinStartup(context);
        await waitForKafkaShutdown(context, this.closeAdmin());
        await Promise.all(this.kafkaEndpoints().map(async (endpoint) => endpoint.stop(context)));
        this.#started = false;
    }
    async stopAdmission(context) {
        if (!this.#started)
            return;
        await this.joinStartup(context);
        await Promise.all(this.kafkaEndpoints().map(async (endpoint) => endpoint.stopAdmission(context)));
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
    #traceAttributes;
    #tracingEnabled;
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
        const tracing = stream.runtimeEnvironment().tracing();
        this.#tracingEnabled = tracing !== undefined;
        this.#tracer = tracing?.tracer(stream.runtimeEnvironment().serviceConfig().name);
        this.#traceAttributes = makeEndpointTraceAttributes(stream, endpoint.name);
    }
    start(context) {
        if (this.#started || this.#tasks.activeCount() !== 0)
            return Promise.reject(new Error(`Kafka endpoint ${this.endpoint().name} already started`));
        this.#tasks = new RuntimeTaskRegistry();
        this.#started = true;
        this.#stopped = false;
        if (this.stream().resultStream() !== undefined) {
            this.#pending = new RotatingMap(PENDING_ROTATION_INTERVAL_MS);
            this.#pending.start(context);
        }
        return Promise.resolve();
    }
    async stop(context) {
        if (!this.#started && !this.#stopped)
            return;
        this.stopAdmission();
        try {
            await waitForKafkaShutdown(context, this.#tasks.drain());
        }
        catch (error) {
            this.#tasks.cancel(error);
            throw error;
        }
        this.#pending?.stop(context);
    }
    stopAdmission() {
        if (!this.#started)
            return;
        this.#started = false;
        this.#stopped = true;
        for (const wake of this.#waiters.splice(0))
            wake();
        this.#tasks.stopAdmission();
    }
    consume(context, value) {
        void context;
        void value;
    }
    handle(context, record, control) {
        if (!this.#started)
            return Promise.resolve();
        return this.#tasks.admit(async (signal) => this.handleOnce(MessageContext.fromContext(context).withExternalCancellation(control.signal), record, control, signal), AbortSignal.any([context.signal(), control.signal]));
    }
    restoreCheckpoint(context) {
        return this.#handler.restoreCheckpoint(MessageContext.fromContext(context), this.#streamContext);
    }
    onError(context, error) {
        return this.#handler.onError(MessageContext.fromContext(context), this.#streamContext, error);
    }
    async handleOnce(context, record, control, signal) {
        await this.acquire(signal);
        try {
            await this.handleAdmitted(context, record, control, signal);
        }
        finally {
            this.#active -= 1;
            this.#waiters.shift()?.();
        }
    }
    async handleAdmitted(owner, record, control, signal) {
        let context = MessageContext.fromContext(owner).withExternalCancellation(signal);
        if (this.#tracingEnabled) {
            const metadata = new Map();
            for (const [name, value] of record.headers) {
                if ([TRACE_SAMPLING_HEADER, "traceparent", "tracestate", "baggage"].includes(name)) {
                    metadata.set(name, Buffer.from(value).toString("utf8"));
                }
            }
            context = context.withMetadata(metadata);
        }
        if (this.#tracingEnabled) {
            context = applyDataSourceEndpointTracing(context, this.stream().runtimeEnvironment(), this.endpoint().id);
        }
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "kafka.input", this.#traceAttributes);
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
            if (span !== undefined)
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
                if (span !== undefined)
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
        if (span !== undefined && failure !== undefined)
            spanError(span, failure);
        try {
            await this.#handler.endRequest(context, this.#streamContext, failure, state);
        }
        catch (error) {
            failure ??= errorFromUnknown(error);
            if (span !== undefined)
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
            let wake;
            try {
                await new Promise((resolve) => {
                    wake = resolve;
                    this.#waiters.push(wake);
                    signal.addEventListener("abort", wake, { once: true });
                    if (signal.aborted)
                        wake();
                });
            }
            finally {
                signal.removeEventListener("abort", wake);
                const index = this.#waiters.indexOf(wake);
                if (index >= 0)
                    this.#waiters.splice(index, 1);
            }
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
function assertKafkaStartupContext(context) {
    if (context.signal().aborted)
        throw errorFromUnknown(context.signal().reason);
    if (context.cancelled())
        throw new ContextDeadlineExceededError();
}
async function waitForKafkaStartup(context, operation) {
    const [result] = await settleWithinDeadline(context, [operation]);
    if (result === undefined) {
        assertKafkaStartupContext(context);
        throw new ContextDeadlineExceededError();
    }
    if (result.status === "rejected")
        throw result.reason;
    assertKafkaStartupContext(context);
}
async function waitForKafkaShutdown(context, operation) {
    const timeoutMs = context.remainingMs();
    const [result] = await settleWithinDeadline(context, [operation]);
    if (result === undefined) {
        if (context.signal().reason instanceof ContextDeadlineExceededError) {
            throw new RuntimeDrainTimeoutError(timeoutMs ?? 0);
        }
        if (context.signal().aborted) {
            throw errorFromUnknown(context.signal().reason);
        }
        throw new RuntimeDrainTimeoutError(timeoutMs ?? 0);
    }
    if (result.status === "rejected")
        throw result.reason;
}
// Use the public native consumer API. The KafkaJS facade catches rebalance/fetch
// errors and silently retries, which cannot implement strict image recovery.
class ConfluentConsumer {
    #consumer;
    #checkpoint;
    #codes;
    #assignments = new Map();
    #commits = new Set();
    #tasks = new Set();
    #topic = "";
    #generation = 0;
    #stopping = false;
    #failure;
    #run;
    #restoring;
    #disconnecting;
    constructor(module, brokers, groupId, connectionTimeoutMs, checkpoint, metrics, security) {
        kafkaSecurityOptions(security); // Validate the same credentials as the other transports.
        this.#checkpoint = checkpoint;
        this.#codes = module.KafkaJS.ErrorCodes;
        const statistics = metrics === undefined ? undefined : librdkafkaStatisticsOptions(metrics, "consumer");
        this.#consumer = new module.KafkaConsumer({
            ...statistics,
            "bootstrap.servers": brokers.join(","), "group.id": groupId,
            "partition.assignment.strategy": "roundrobin",
            "isolation.level": "read_committed",
            "enable.auto.commit": true, "enable.auto.offset.store": false,
            "auto.offset.reset": checkpoint.restoring ? "error" : "earliest",
            ...(connectionTimeoutMs === 0 ? {} : { "socket.timeout.ms": connectionTimeoutMs }),
            ...(security === undefined ? {} : {
                "security.protocol": security.protocol.toLowerCase(),
                ...(security.protocol === "PLAINTEXT" ? {} : {
                    "sasl.mechanisms": security.mechanism,
                    ...(security.username === undefined ? {} : { "sasl.username": security.username }),
                    ...(security.password === undefined ? {} : { "sasl.password": security.password })
                })
            }),
            rebalance_cb: (error, partitions) => {
                this.rebalance(error, partitions);
            },
            offset_commit_cb: (error, offsets) => {
                for (const commit of this.#commits) {
                    if (error || offsets.some((offset) => offset.topic === this.#topic &&
                        offset.partition === commit.partition && offset.offset === commit.next)) {
                        this.#commits.delete(commit);
                        if (error)
                            commit.reject(new Error(error.message, { cause: error }));
                        else
                            commit.resolve();
                    }
                }
            }
        }, {});
        this.#consumer.setDefaultConsumeTimeout(100);
        this.#consumer.on("event.error", (error) => this.fail(error));
        this.#consumer.on("rebalance.error", (error) => this.fail(error));
    }
    connect() {
        return new Promise((resolve, reject) => {
            this.#consumer.connect({}, (error) => { if (error)
                reject(error);
            else
                resolve(); });
        });
    }
    async subscribe(topic) {
        this.#topic = topic;
        this.#consumer.subscribe([topic]);
    }
    invalidate() {
        ++this.#generation;
        const old = [...this.#assignments.values()];
        this.#assignments.clear();
        for (const assignment of old)
            assignment.controller.abort(new Error("Kafka partition revoked"));
    }
    fail(failure) {
        const code = "code" in failure ? failure.code : undefined;
        let error = failure instanceof Error ? failure : new Error(failure.message, { cause: failure });
        if (this.#checkpoint.restoring &&
            (code === this.#codes.ERR_OFFSET_OUT_OF_RANGE || code === this.#codes.ERR__AUTO_OFFSET_RESET)) {
            error = new KafkaCheckpointError(`Kafka checkpoint lost: ${error.message}`, { cause: error });
        }
        this.#failure ??= error;
        this.invalidate();
        for (const commit of this.#commits)
            commit.reject(error);
        this.#commits.clear();
    }
    rebalance(error, partitions) {
        this.invalidate();
        const generation = this.#generation;
        if (error.code === this.#codes.ERR__REVOKE_PARTITIONS) {
            try {
                this.#consumer.unassign();
            }
            catch (failure) {
                this.fail(errorFromUnknown(failure));
            }
            return;
        }
        if (error.code !== this.#codes.ERR__ASSIGN_PARTITIONS) {
            this.fail(error);
            return;
        }
        // Native callbacks do not apply an implicit assignment on return. The
        // assignment stays closed while bounded metadata requests validate its cut.
        const operation = this.restoreAssignment(generation, partitions);
        this.#restoring = operation;
        void operation.catch((failure) => {
            if (generation !== this.#generation || this.#stopping)
                return;
            this.fail(new KafkaCheckpointError(`Kafka checkpoint restoration failed: ${errorFromUnknown(failure).message}`, { cause: failure }));
            try {
                this.#consumer.unassign();
            }
            catch { /* admission is already closed */ }
        }).finally(() => { if (this.#restoring === operation)
            this.#restoring = undefined; });
    }
    async restoreAssignment(generation, partitions) {
        const selected = await this.#checkpoint.restore();
        validateCheckpoint(selected);
        if ((selected !== null) !== this.#checkpoint.restoring)
            throw new KafkaCheckpointError("Kafka recovery mode changed during lifecycle");
        // Copy the provider's map before the first asynchronous metadata request.
        const offsets = selected === null ? null : new Map(selected);
        const assignment = [];
        for (const partition of partitions) {
            if (generation !== this.#generation || this.#stopping || this.#failure || this.#checkpoint.context.cancelled())
                return;
            if (partition.topic !== this.#topic)
                throw new KafkaCheckpointError("Kafka assigned an unexpected topic");
            if (offsets === null) {
                assignment.push(partition);
                continue;
            }
            const next = offsets.get(partition.partition);
            if (next === undefined)
                throw new KafkaCheckpointError("Kafka checkpoint does not cover assigned partition");
            const bounds = await new Promise((resolve, reject) => {
                this.#consumer.queryWatermarkOffsets(partition.topic, partition.partition, 5000, (error, value) => { if (error)
                    reject(error);
                else
                    resolve(value); });
            });
            if (!Number.isSafeInteger(bounds.lowOffset) || !Number.isSafeInteger(bounds.highOffset) ||
                bounds.lowOffset < 0 || next < BigInt(bounds.lowOffset) || next > BigInt(bounds.highOffset))
                throw new KafkaCheckpointError("Kafka checkpoint is outside retained log bounds");
            assignment.push({ ...partition, offset: Number(next) });
        }
        if (generation !== this.#generation || this.#stopping || this.#failure || this.#checkpoint.context.cancelled())
            return;
        // Let already submitted old-generation commits settle before installing
        // a new checkpoint on this same native consumer.
        await Promise.allSettled([...this.#commits].map((commit) => commit.done));
        if (generation !== this.#generation || this.#stopping || this.#failure || this.#checkpoint.context.cancelled())
            return;
        this.#consumer.assign(assignment);
        for (const partition of partitions)
            this.#assignments.set(partition.partition, { controller: new AbortController() });
    }
    run(concurrency, handler) {
        if (this.#run !== undefined)
            return Promise.reject(new Error("Kafka consumer already running"));
        this.#run = this.consume(concurrency, handler);
        return this.#run;
    }
    async consume(_concurrency, handler) {
        try {
            while ((!this.#stopping || this.#tasks.size > 0) && !this.#failure && !this.#checkpoint.context.cancelled()) {
                const messages = await new Promise((resolve, reject) => {
                    // Keep the previous KafkaJS facade's 32-record fetch batch. Per-stream
                    // concurrency remains the endpoint handler's existing control.
                    this.#consumer.consume(32, (error, values) => { if (error)
                        reject(error);
                    else
                        resolve(values); });
                });
                // Keep polling while accepted callbacks drain: native asynchronous
                // commit completion is dispatched by consumer polling.
                if (this.#stopping)
                    continue;
                const partitions = new Map();
                for (const message of messages) {
                    if (message.topic !== this.#topic || !this.#assignments.has(message.partition))
                        continue;
                    const values = partitions.get(message.partition) ?? [];
                    values.push(message);
                    partitions.set(message.partition, values);
                }
                for (const [partition, values] of partitions) {
                    const lease = this.#assignments.get(partition);
                    this.#consumer.pause([{ topic: this.#topic, partition }]);
                    const task = this.deliver(values, lease, handler);
                    this.#tasks.add(task);
                    void task.catch((error) => {
                        if (this.current(partition, lease))
                            this.fail(errorFromUnknown(error));
                    }).finally(() => {
                        this.#tasks.delete(task);
                        if (this.current(partition, lease) && !this.#stopping) {
                            try {
                                this.#consumer.resume([{ topic: this.#topic, partition }]);
                            }
                            catch (error) {
                                this.fail(errorFromUnknown(error));
                            }
                        }
                    });
                }
            }
        }
        catch (error) {
            this.fail(errorFromUnknown(error));
        }
        if (this.#failure || this.#checkpoint.context.cancelled())
            this.invalidate();
        await Promise.allSettled([...this.#tasks]);
        if (this.#failure)
            throw this.#failure;
    }
    current(partition, lease) {
        return !this.#failure && !lease.controller.signal.aborted &&
            !this.#checkpoint.context.cancelled() && this.#assignments.get(partition) === lease;
    }
    async deliver(messages, lease, handler) {
        for (const message of messages) {
            if (!this.current(message.partition, lease))
                return;
            if (!Number.isSafeInteger(message.offset) || !Number.isSafeInteger(message.offset + 1))
                throw new KafkaCheckpointError("Kafka native offset cannot be represented exactly");
            const record = nativeKafkaRecord(message);
            let marked;
            let committed = false;
            const assertCurrent = () => {
                if (!this.current(message.partition, lease))
                    throw new Error("Kafka message belongs to a revoked assignment");
            };
            const commit = async (metadata) => {
                assertCurrent();
                let resolve;
                let reject;
                const done = new Promise((accept, fail) => { resolve = accept; reject = fail; });
                const pending = { partition: message.partition, next: message.offset + 1, done, resolve, reject };
                this.#commits.add(pending);
                try {
                    this.#consumer.commit({ topic: message.topic, partition: message.partition,
                        offset: message.offset + 1, ...(metadata === undefined ? {} : { metadata }) });
                }
                catch (error) {
                    this.#commits.delete(pending);
                    reject(errorFromUnknown(error));
                }
                await done;
                committed = true;
            };
            const control = {
                signal: lease.controller.signal,
                mark: (candidate, metadata) => {
                    if (candidate !== record)
                        throw new Error("Kafka control belongs to another message");
                    assertCurrent();
                    marked = metadata;
                },
                commit: async (candidate) => {
                    if (candidate !== record)
                        throw new Error("Kafka control belongs to another message");
                    await commit();
                }
            };
            await handler(record, control);
            if (!this.current(message.partition, lease))
                return;
            if (committed)
                continue;
            if (marked === undefined) {
                await new Promise((resolve, reject) => this.#consumer.seek({ topic: message.topic,
                    partition: message.partition, offset: message.offset }, 5000, (error) => { if (error)
                    reject(error);
                else
                    resolve(); }));
                return;
            }
            if (marked.length > 0)
                await commit(marked);
            else
                this.#consumer.offsetsStore([{ topic: message.topic, partition: message.partition, offset: message.offset + 1 }]);
        }
    }
    async stop(context) {
        this.#stopping = true;
        const partitions = [...this.#assignments.keys()].map((partition) => ({ topic: this.#topic, partition }));
        if (partitions.length > 0)
            this.#consumer.pause(partitions);
        await waitForKafkaShutdown(context, this.#run ?? Promise.resolve());
    }
    disconnect() {
        if (this.#disconnecting === undefined) {
            const operation = this.disconnectOnce();
            this.#disconnecting = operation;
            void operation.catch(() => { this.#disconnecting = undefined; });
        }
        return this.#disconnecting;
    }
    async disconnectOnce() {
        this.#stopping = true;
        this.invalidate();
        for (const commit of this.#commits)
            commit.reject(new Error("Kafka consumer disconnected"));
        this.#commits.clear();
        await Promise.allSettled([...(this.#run ? [this.#run] : []), ...(this.#restoring ? [this.#restoring] : [])]);
        if (!this.#consumer.isConnected())
            return;
        await new Promise((resolve, reject) => this.#consumer.disconnect((error) => {
            if (error)
                reject(error);
            else
                resolve();
        }));
    }
}
function nativeKafkaRecord(message) {
    const headers = new Map();
    for (const header of message.headers ?? []) {
        for (const [key, value] of Object.entries(header))
            if (value !== undefined)
                headers.set(key, Buffer.from(value));
    }
    return { topic: message.topic, partition: message.partition, offset: BigInt(message.offset),
        key: message.key === null || message.key === undefined ? undefined : Buffer.from(message.key),
        value: message.value === null ? new Uint8Array() : new Uint8Array(message.value), headers };
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