import { Cron } from "croner";
import { Context, DataConnectorType, DataSourceEndpoint, DataSourceEndpointConsumer, InputDataSource, MessageContext, ScheduleBackend, err, makeScheduleTrigger, newStreamId, str } from "../../runtime/index.js";
class CronEndpoint extends DataSourceEndpoint {
    #binding;
    #job;
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
        const job = new Cron(config.schedule, {
            name: `${this.dataSource().name}.${this.name}`,
            paused: true,
            timezone: config.timezone,
            protect: config.overlapPolicy === "Skip",
            catch: (failure) => {
                this.runtimeEnvironment()
                    .log()
                    .error(Context.background(), "cron endpoint execution failed", str("endpoint", this.name), err(failure instanceof Error ? failure : new Error(String(failure))));
            }
        }, async (current) => {
            const firedAt = new Date().toISOString();
            const scheduledAt = (current.currentRun() ?? new Date()).toISOString();
            const context = new MessageContext().withStreamId(newStreamId());
            const started = this.onRequestStart(context);
            let failure;
            try {
                const trigger = makeScheduleTrigger(this.id, this.name, scheduledAt, firedAt, ScheduleBackend.Local);
                await binding.consume(context, trigger);
            }
            catch (error) {
                failure = error instanceof Error ? error : new Error(String(error));
                throw failure;
            }
            finally {
                this.onRequestEnd(context, started, failure);
            }
        });
        this.#job = job;
        job.resume();
    }
    stop() {
        this.#job?.stop();
        this.#job = undefined;
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
                endpoint.stop();
            return Promise.reject(error instanceof Error ? error : new Error(String(error)));
        }
    }
    stop(_context) {
        void _context;
        if (!this.#started)
            return Promise.resolve();
        this.#started = false;
        for (const endpoint of this.cronEndpoints())
            endpoint.stop();
        return Promise.resolve();
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
export function makeCronEndpointConsumer(stream) {
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
    const consumer = new DataSourceEndpointConsumer(endpoint, stream);
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