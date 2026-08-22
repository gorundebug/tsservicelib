import { DataConnectorType, DataSourceEndpoint, DataSourceEndpointConsumer, FunctionCollector, InputDataSource, Context, RotatingMap, RuntimeTaskRegistry, err, errorFromUnknown, makeStreamContext, newStreamId, spanError, str, stringAttribute } from "../../runtime/index.js";
const PENDING_ROTATION_INTERVAL_MS = 30_000;
class CustomResult {
    state;
    #callbacks = new Map();
    #done;
    #resolveDone;
    #completed = false;
    #retiring = false;
    #activeCallbacks = 0;
    #retired;
    #resolveRetired;
    constructor(state) {
        this.state = state;
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
        this.#resolveDone?.();
        this.#resolveDone = undefined;
    }
    wait() {
        return this.#done;
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
class CustomSourceEndpoint extends DataSourceEndpoint {
    #producer;
    #producerTasks;
    #binding;
    #started = false;
    constructor(dataSource, endpointId, producer) {
        super(dataSource, endpointId);
        this.#producer = producer;
        this.#producerTasks = new RuntimeTaskRegistry((error) => {
            this.runtimeEnvironment()
                .log()
                .error(Context.background(), "data producer error", str("endpoint", this.name), err(error));
        });
    }
    bind(binding) {
        if (this.#binding !== undefined) {
            throw new Error(`consumer already assigned to custom endpoint ${this.name}`);
        }
        this.#binding = binding;
        this.addEndpointConsumer(binding);
    }
    consume(context, value) {
        return this.#binding?.handle(context, value);
    }
    async start(context) {
        if (this.#started)
            throw new Error(`custom endpoint ${this.name} is already started`);
        const binding = this.#binding;
        if (binding === undefined)
            throw new Error(`custom endpoint ${this.name} has no consumer`);
        await binding.start(context);
        this.#started = true;
        this.#producerTasks.admitDetached(async (signal) => {
            await this.#producer.start(context.withExternalCancellation(signal), this);
        });
    }
    async stop(context) {
        if (!this.#started)
            return;
        this.#started = false;
        await this.#binding?.stop(context);
        this.#producerTasks.cancel(new Error(`custom endpoint ${this.name} stopped`));
        try {
            await this.#producer.stop(context);
        }
        finally {
            await this.#producerTasks.drain(context.remainingMs());
        }
    }
}
export class CustomDataSource extends InputDataSource {
    #started = false;
    constructor(connectorId, environment) {
        super(connectorId, environment);
        if (this.config().type !== DataConnectorType.Custom) {
            throw new Error(`data source ${this.name} is not custom`);
        }
    }
    async start(context) {
        if (this.#started)
            throw new Error(`custom data source ${this.name} is already started`);
        this.#started = true;
        try {
            for (const endpoint of this.customEndpoints())
                await endpoint.start(context);
        }
        catch (error) {
            this.#started = false;
            await Promise.allSettled(this.customEndpoints().map(async (endpoint) => endpoint.stop(Context.background())));
            throw error;
        }
    }
    async stop(context) {
        if (!this.#started)
            return;
        this.#started = false;
        await Promise.all(this.customEndpoints().map(async (endpoint) => endpoint.stop(context)));
    }
    customEndpoints() {
        return this.endpoints().map((endpoint) => {
            if (!(endpoint instanceof CustomSourceEndpoint)) {
                throw new Error(`source endpoint ${endpoint.name} is not custom`);
            }
            return endpoint;
        });
    }
}
class CustomEndpointConsumer extends DataSourceEndpointConsumer {
    #streamContext;
    #handler;
    #tasks = new RuntimeTaskRegistry();
    #waiters = [];
    #tracer;
    #pending;
    #active = 0;
    #started = false;
    #stopped = false;
    constructor(endpoint, stream, handler) {
        super(endpoint, stream);
        this.#handler = handler;
        this.#streamContext = makeStreamContext(stream, stream.resultStream(), new FunctionCollector((context, value) => stream.consume(context, value)), new FunctionCollector((context, value) => stream.errorStream().consume(context, value)));
        if (stream.resultStream() !== undefined) {
            stream.setResultConsumer({
                consume: (context, value) => this.consumeResult(context, value)
            });
        }
        this.#tracer = stream
            .runtimeEnvironment()
            .tracing()
            ?.tracer(stream.runtimeEnvironment().serviceConfig().name);
    }
    start(context) {
        if (this.#started) {
            return Promise.reject(new Error(`custom endpoint ${this.endpoint().name} already started`));
        }
        this.#started = true;
        this.#stopped = false;
        if (this.stream().resultStream() !== undefined) {
            this.#pending = new RotatingMap(PENDING_ROTATION_INTERVAL_MS);
            this.#pending.start(context);
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
        this.#tasks.cancel(context.signal().reason ?? new Error("custom endpoint stopped"));
        try {
            await this.#tasks.drain(context.remainingMs());
        }
        finally {
            this.#pending?.stop(context);
        }
    }
    consume(context, value) {
        return this.stream().consume(context, value);
    }
    handle(context, value) {
        if (!this.#started)
            return Promise.resolve();
        return this.#tasks.admit(async (signal) => this.handleOnce(context, value, signal), context.signal());
    }
    async handleOnce(context, value, signal) {
        await this.acquire(signal);
        try {
            await this.handleAdmitted(context.withExternalCancellation(signal), value);
        }
        finally {
            this.#active -= 1;
            this.#waiters.shift()?.();
        }
    }
    async handleAdmitted(context, value) {
        let span;
        if (this.#tracer !== undefined && context.samplingEnabled()) {
            const started = this.#tracer.start(context, "local.input", [
                stringAttribute("stream", this.stream().name),
                stringAttribute("endpoint", this.endpoint().name)
            ]);
            context = started.context;
            span = started.span;
        }
        try {
            await this.handleTraced(context, value, span);
        }
        finally {
            span?.end();
        }
    }
    async handleTraced(context, value, span) {
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
        const result = new CustomResult(state);
        const hasResult = this.stream().resultStream() !== undefined;
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
            await this.#handler.consumeMessage(context, this.#streamContext, state, value, result);
            span?.addEvent("consume_message");
        }
        catch (error) {
            failure = errorFromUnknown(error);
            span?.addEvent("consume_message.error", [stringAttribute("error", failure.message)]);
        }
        if (failure === undefined && hasResult) {
            try {
                await waitForResult(result, context.signal());
                span?.addEvent("done_received");
            }
            catch (error) {
                failure = errorFromUnknown(error);
                resultWaitFailed = true;
            }
        }
        if (hasResult) {
            const resultCompleted = await result.retire();
            if (resultWaitFailed && resultCompleted) {
                failure = undefined;
                span?.addEvent("done_received");
            }
            this.pending().pop(streamId);
            this.endpoint().onPendingRemove(context, streamId);
        }
        if (failure !== undefined) {
            spanError(span, failure);
            if (context.cancelled()) {
                span?.addEvent("context_cancelled", [stringAttribute("error", failure.message)]);
            }
        }
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
                    ? new Error("custom endpoint stopped")
                    : errorFromUnknown(signal.reason);
            }
            const concurrency = this.#handler.concurrency(this.#streamContext);
            if (concurrency < 0 || !Number.isSafeInteger(concurrency)) {
                throw new RangeError("custom endpoint concurrency must be a non-negative safe integer");
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
        if (!found || result?.beginCallback() !== true) {
            this.endpoint().onLateResult(context, streamId);
            return;
        }
        try {
            const messageId = this.#handler.getMessageId(context, this.#streamContext, result.state, value);
            const callback = result.callback(messageId);
            if (callback === undefined) {
                this.endpoint().onUnknownMessageId(context, streamId, messageId);
                return;
            }
            if (await callback(context, this.#streamContext, result.state, value)) {
                if (!result.remove(messageId, callback)) {
                    this.endpoint().onDuplicateMessageId(context, streamId, messageId);
                }
            }
        }
        finally {
            result.endCallback();
        }
    }
    pending() {
        if (this.#pending === undefined) {
            throw new Error(`custom endpoint ${this.endpoint().name} pending store is not started`);
        }
        return this.#pending;
    }
}
export function makeCustomEndpointConsumer(stream, producer, handler) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
    if (endpointConfig === undefined) {
        throw new Error(`custom endpoint config ${String(stream.endpointId())} not found`);
    }
    const dataSource = getOrCreateDataSource(endpointConfig.idDataConnector, environment);
    if (dataSource.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new CustomSourceEndpoint(dataSource, endpointConfig.id, producer);
    const consumer = new CustomEndpointConsumer(endpoint, stream, handler);
    endpoint.bind(consumer);
    dataSource.addEndpoint(endpoint);
    return consumer;
}
function getOrCreateDataSource(connectorId, environment) {
    const existing = environment.dataSourceById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof CustomDataSource)) {
            throw new Error(`data source ${String(connectorId)} is not custom`);
        }
        return existing;
    }
    const dataSource = new CustomDataSource(connectorId, environment);
    environment.addDataSource(dataSource);
    return dataSource;
}
async function waitForResult(result, signal) {
    if (signal.aborted) {
        throw signal.reason === undefined
            ? new Error("custom source request cancelled")
            : errorFromUnknown(signal.reason);
    }
    let cancelled;
    try {
        await Promise.race([
            result.wait(),
            new Promise((_resolve, reject) => {
                cancelled = () => {
                    reject(signal.reason === undefined
                        ? new Error("custom source request cancelled")
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
//# sourceMappingURL=custom.js.map