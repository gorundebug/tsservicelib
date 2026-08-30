import { Cron } from "croner";
import { Context, DataConnectorType, DataSourceEndpoint, FunctionCollector, InputDataSource, MessageContext, ScheduleBackend, applyDataSourceEndpointTracing, err, makeScheduleTrigger, newStreamId, str } from "../../runtime/index.js";
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
    #running = false;
    #active = new Set();
    bind(binding) {
        if (this.#binding !== undefined) {
            throw new Error(`consumer already assigned to cron endpoint ${this.name}`);
        }
        this.#binding = binding;
        this.addEndpointConsumer(binding);
    }
    start() {
        if (this.#job !== undefined)
            throw new Error(`cron endpoint ${this.name} is already started`);
        const config = this.cronConfig();
        if (!config.enabled)
            return;
        const binding = this.#binding;
        if (binding === undefined)
            throw new Error(`cron endpoint ${this.name} has no consumer`);
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
            return this.#startDispatch(binding, scheduledAt);
        });
        this.#job = job;
        nextScheduledAt = job.nextRun();
        if (nextScheduledAt === null) {
            job.stop();
            this.#job = undefined;
            throw new Error(`cron endpoint ${this.name} has no next occurrence`);
        }
    }
    async stop(context = Context.background()) {
        this.#job?.stop();
        this.#job = undefined;
        const drain = Promise.allSettled(this.#active);
        const remainingMs = context.remainingMs();
        if (remainingMs === undefined) {
            await drain;
            return;
        }
        let timer;
        const timeout = new Promise((resolve) => {
            timer = setTimeout(resolve, Math.max(0, remainingMs));
        });
        try {
            const outcome = await Promise.race([
                drain.then(() => "drained"),
                timeout.then(() => "timeout")
            ]);
            if (outcome === "timeout") {
                this.runtimeEnvironment()
                    .log()
                    .warn(context, "cron endpoint stopped by shutdown timeout", str("endpoint", this.name));
            }
        }
        finally {
            if (timer !== undefined)
                clearTimeout(timer);
        }
    }
    #startDispatch(binding, scheduledAt) {
        const execution = this.#dispatch(binding, scheduledAt);
        this.#active.add(execution);
        return execution.finally(() => {
            this.#active.delete(execution);
        });
    }
    async #dispatch(binding, scheduledAt) {
        const config = this.cronConfig();
        if (this.#running && config.overlapPolicy === "Skip")
            return;
        this.#running = true;
        const context = applyDataSourceEndpointTracing(new MessageContext().withStreamId(newStreamId()), this.runtimeEnvironment(), this.id);
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
                .error(Context.background(), "cron endpoint execution failed", str("endpoint", this.name), err(failure));
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
    constructor(connectorId, environment) {
        super(connectorId, environment);
        if (this.config().type !== DataConnectorType.Cron) {
            throw new Error(`data source ${this.name} is not cron`);
        }
    }
    start(_context) {
        void _context;
        if (this.#started)
            return Promise.reject(new Error(`cron data source ${this.name} is already started`));
        this.#started = true;
        try {
            for (const endpoint of this.cronEndpoints())
                endpoint.start();
            return Promise.resolve();
        }
        catch (error) {
            this.#started = false;
            for (const endpoint of this.cronEndpoints())
                void endpoint.stop(_context);
            return Promise.reject(error instanceof Error ? error : new Error(String(error)));
        }
    }
    stop(context) {
        if (!this.#started)
            return Promise.resolve();
        this.#started = false;
        return Promise.all(this.cronEndpoints().map(async (endpoint) => endpoint.stop(context))).then(() => undefined);
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