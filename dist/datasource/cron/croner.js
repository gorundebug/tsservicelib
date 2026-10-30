import { Cron } from "croner";
import { ContextDeadlineExceededError, DataConnectorType, DataSourceEndpoint, FunctionCollector, InputDataSource, MessageContext, RuntimeDrainTimeoutError, ScheduleBackend, applyDataSourceEndpointTracing, err, makeScheduleTrigger, newStreamId, settleWithinDeadline, str } from "../../runtime/index.js";
/** Canonical local interval; calendar expressions stay with Croner. */
export function parseLocalScheduleInterval(expression) {
    if (!expression.startsWith("@every"))
        return undefined;
    const match = /^@every ([1-9][0-9]{0,7})ms$/u.exec(expression);
    const milliseconds = match === null ? Number.NaN : Number(match[1]);
    if (!Number.isInteger(milliseconds) || milliseconds < 10 || milliseconds > 86_400_000) {
        throw new RangeError("local interval requires @every <10..86400000>ms");
    }
    return milliseconds;
}
class CronEndpointConsumer {
    #endpoint;
    #function;
    #collector;
    #hasResult;
    #pending = new Map();
    constructor(endpoint, stream, function_) {
        this.#endpoint = endpoint;
        this.#function = function_;
        this.#collector = new FunctionCollector((context, value) => stream.consume(context, value));
        this.#hasResult = stream.resultStream() !== undefined;
        if (this.#hasResult) {
            stream.setResultConsumer({
                consume: (context) => {
                    this.consumeResult(context);
                }
            });
        }
    }
    endpoint() {
        return this.#endpoint;
    }
    async consume(context, trigger) {
        if (!this.#hasResult) {
            await this.#function.onTrigger(context, trigger, this.#collector);
            return;
        }
        const streamId = context.streamId();
        if (streamId === undefined || streamId === "") {
            throw new Error(`cron endpoint ${this.#endpoint.name} activation has no stream ID`);
        }
        if (this.#pending.has(streamId)) {
            throw new Error(`cron endpoint ${this.#endpoint.name} already has an active execution ${streamId}`);
        }
        const resolution = Promise.withResolvers();
        const pending = { ...resolution, settled: false };
        this.#pending.set(streamId, pending);
        this.#endpoint.onPendingAdd(context, streamId);
        try {
            await this.#function.onTrigger(context, trigger, this.#collector);
            await pending.promise;
        }
        finally {
            this.#pending.delete(streamId);
            this.#endpoint.onPendingRemove(context, streamId);
        }
    }
    consumeResult(context) {
        const streamId = context.streamId();
        if (streamId === undefined || streamId === "") {
            this.#endpoint.onMissingStreamId(context);
            return;
        }
        const pending = this.#pending.get(streamId);
        if (pending === undefined) {
            this.#endpoint.onLateResult(context, streamId);
            return;
        }
        if (pending.settled) {
            this.#endpoint.onDuplicateMessageId(context, streamId, streamId);
            return;
        }
        pending.settled = true;
        pending.resolve(undefined);
    }
}
class CronEndpoint extends DataSourceEndpoint {
    #binding;
    #job;
    #intervalStop;
    #running = false;
    #active = new Set();
    bind(binding) {
        if (this.#binding !== undefined) {
            throw new Error(`consumer already assigned to cron endpoint ${this.name}`);
        }
        this.#binding = binding;
        this.addEndpointConsumer(binding);
    }
    start(context) {
        if (this.#job !== undefined || this.#intervalStop !== undefined || this.#active.size !== 0)
            throw new Error(`cron endpoint ${this.name} is already started`);
        const config = this.cronConfig();
        if (!config.enabled)
            return;
        const binding = this.#binding;
        if (binding === undefined)
            throw new Error(`cron endpoint ${this.name} has no consumer`);
        const interval = parseLocalScheduleInterval(config.schedule);
        if (interval !== undefined) {
            if (config.timezone !== "UTC")
                throw new Error("local interval requires timezone UTC");
            this.#startInterval(context, binding, interval);
            return;
        }
        let nextScheduledAt = null;
        const job = new Cron(config.schedule, {
            name: `${this.dataSource().name}.${this.name}`,
            timezone: config.timezone,
            protect: config.overlapPolicy === "Skip"
        }, (current) => {
            const scheduledAt = nextScheduledAt ?? current.currentRun() ?? new Date();
            const next = current.nextRun(scheduledAt);
            nextScheduledAt = next;
            const currentConfig = this.cronConfig();
            if (currentConfig.missedRunPolicy === "Skip" &&
                next !== null &&
                next.getTime() <= Date.now()) {
                return;
            }
            if (context.cancelled())
                return;
            return this.#startDispatch(context, binding, scheduledAt);
        });
        this.#job = job;
        nextScheduledAt = job.nextRun();
        if (nextScheduledAt === null) {
            job.stop();
            this.#job = undefined;
            throw new Error(`cron endpoint ${this.name} has no next occurrence`);
        }
    }
    async stop(context) {
        this.#intervalStop?.();
        this.#intervalStop = undefined;
        this.#job?.stop();
        this.#job = undefined;
        const remainingMs = context.remainingMs();
        const results = await settleWithinDeadline(context, [...this.#active]);
        if (results.some((result) => result === undefined)) {
            const reason = context.signal().reason;
            if (context.signal().aborted && !(reason instanceof ContextDeadlineExceededError)) {
                throw reason instanceof Error
                    ? reason
                    : new Error("cron shutdown cancelled", { cause: reason });
            }
            throw new RuntimeDrainTimeoutError(remainingMs ?? 0);
        }
        for (const result of results) {
            if (result?.status === "rejected")
                throw result.reason;
        }
    }
    #startInterval(owner, binding, milliseconds) {
        let next = Date.now() + milliseconds;
        let timer;
        let stopped = false;
        const signal = owner.signal();
        const stop = () => {
            stopped = true;
            if (timer !== undefined)
                clearTimeout(timer);
            signal.removeEventListener("abort", stop);
        };
        const arm = () => {
            const delay = Math.max(0, Math.min(next - Date.now(), owner.remainingMs() ?? Infinity, 2_147_483_647));
            timer = setTimeout(fire, delay);
        };
        const fire = () => {
            if (stopped || owner.cancelled()) {
                stop();
                return;
            }
            const count = Math.floor((Date.now() - next) / milliseconds) + 1;
            if (count < 1) {
                arm();
                return;
            }
            const scheduledAt = new Date(next + (count - 1) * milliseconds);
            next += count * milliseconds;
            arm();
            if (count === 1 || this.cronConfig().missedRunPolicy === "FireOnce") {
                void this.#startDispatch(owner, binding, scheduledAt).catch((failure) => {
                    this.runtimeEnvironment().log().error(owner, "interval endpoint dispatch failed", err(failure instanceof Error ? failure : new Error(String(failure))));
                });
            }
        };
        this.#intervalStop = stop;
        signal.addEventListener("abort", stop, { once: true });
        if (owner.cancelled())
            stop();
        else
            arm();
    }
    #startDispatch(owner, binding, scheduledAt) {
        const execution = this.#dispatch(owner, binding, scheduledAt);
        this.#active.add(execution);
        return execution.finally(() => {
            this.#active.delete(execution);
        });
    }
    async #dispatch(owner, binding, scheduledAt) {
        const config = this.cronConfig();
        if (this.#running && config.overlapPolicy === "Skip")
            return;
        this.#running = true;
        const context = applyDataSourceEndpointTracing((owner instanceof MessageContext ? owner : MessageContext.fromContext(owner)).withStreamId(newStreamId()), this.runtimeEnvironment(), this.id);
        const started = this.onRequestStart(context);
        let failure;
        try {
            const trigger = makeScheduleTrigger(this.id, this.name, scheduledAt.toISOString(), new Date().toISOString(), ScheduleBackend.Local);
            await binding.consume(context, trigger);
        }
        catch (error) {
            failure = error instanceof Error ? error : new Error(String(error));
            this.runtimeEnvironment()
                .log()
                .error(context, "cron endpoint execution failed", str("endpoint", this.name), err(failure));
        }
        finally {
            this.#running = false;
            this.onRequestEnd(context, started, failure);
        }
    }
    cronConfig() {
        const config = this.config();
        if (!isCronEndpointConfig(config)) {
            throw new Error(`endpoint ${this.name} is not a local cron endpoint`);
        }
        return config;
    }
}
function isCronEndpointConfig(config) {
    return ("schedule" in config &&
        !("taskQueue" in config) &&
        typeof config.schedule === "string" &&
        "enabled" in config &&
        typeof config.enabled === "boolean" &&
        "timezone" in config &&
        typeof config.timezone === "string" &&
        "overlapPolicy" in config &&
        (config.overlapPolicy === "Allow" || config.overlapPolicy === "Skip") &&
        "missedRunPolicy" in config &&
        (config.missedRunPolicy === "FireOnce" || config.missedRunPolicy === "Skip"));
}
export class CronDataSource extends InputDataSource {
    #started = false;
    #generation = 0;
    constructor(connectorId, environment) {
        super(connectorId, environment);
        if (this.config().type !== DataConnectorType.Cron) {
            throw new Error(`data source ${this.name} is not cron`);
        }
    }
    async start(context) {
        if (context.cancelled()) {
            const reason = context.signal().reason;
            return Promise.reject(reason instanceof Error
                ? reason
                : new Error("cron source context cancelled", { cause: reason }));
        }
        if (this.#started)
            return Promise.reject(new Error(`cron data source ${this.name} is already started`));
        this.#started = true;
        const generation = ++this.#generation;
        try {
            for (const endpoint of this.cronEndpoints())
                endpoint.start(context);
            return;
        }
        catch (error) {
            const cleanup = await Promise.allSettled(this.cronEndpoints().map(async (endpoint) => endpoint.stop(context)));
            if (this.#generation === generation) {
                this.#started = cleanup.some((result) => result.status === "rejected");
            }
            throw error instanceof Error ? error : new Error(String(error));
        }
    }
    async stop(context) {
        if (!this.#started)
            return;
        const generation = this.#generation;
        await Promise.all(this.cronEndpoints().map(async (endpoint) => endpoint.stop(context)));
        if (this.#generation === generation)
            this.#started = false;
    }
    cronEndpoints() {
        return this.endpoints().map((endpoint) => {
            if (!(endpoint instanceof CronEndpoint)) {
                throw new Error(`source endpoint ${endpoint.name} is not cron`);
            }
            return endpoint;
        });
    }
}
export function makeCronEndpointConsumer(stream, function_) {
    const environment = stream.runtimeEnvironment();
    const endpointConfig = environment.runtimeConfig().endpointById(stream.endpointId());
    if (endpointConfig === undefined) {
        throw new Error(`cron endpoint config ${String(stream.endpointId())} not found`);
    }
    const dataSource = getOrCreateCronDataSource(endpointConfig.idDataConnector, environment);
    if (dataSource.endpoint(endpointConfig.id) !== undefined) {
        throw new Error(`endpoint ${endpointConfig.name} already exists`);
    }
    const endpoint = new CronEndpoint(dataSource, endpointConfig.id);
    const consumer = new CronEndpointConsumer(endpoint, stream, function_);
    endpoint.bind(consumer);
    dataSource.addEndpoint(endpoint);
    return consumer;
}
function getOrCreateCronDataSource(connectorId, environment) {
    const existing = environment.dataSourceById(connectorId);
    if (existing !== undefined) {
        if (!(existing instanceof CronDataSource)) {
            throw new Error(`data source ${String(connectorId)} is not cron`);
        }
        return existing;
    }
    const dataSource = new CronDataSource(connectorId, environment);
    environment.addDataSource(dataSource);
    return dataSource;
}
//# sourceMappingURL=croner.js.map