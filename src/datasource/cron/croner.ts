import { Cron } from "croner";

import {
  Context,
  DataConnectorType,
  DataSourceEndpoint,
  FunctionCollector,
  InputDataSource,
  MessageContext,
  ScheduleBackend,
  applyDataSourceEndpointTracing,
  err,
  makeScheduleTrigger,
  newStreamId,
  str,
  type Consumer,
  type CronEndpointConfig,
  type EndpointConfig,
  type InputEndpointConsumer,
  type RuntimeEnvironment,
  type ScheduleEndpointFunction,
  type ScheduleTrigger,
  type TypedInputStream
} from "../../runtime/index.js";

type CronEndpointBinding = InputEndpointConsumer & Consumer<ScheduleTrigger>;

class CronEndpointConsumer<T, R, E> implements InputEndpointConsumer, Consumer<ScheduleTrigger> {
  readonly #endpoint: DataSourceEndpoint;
  readonly #function: ScheduleEndpointFunction<T>;
  readonly #collector: FunctionCollector<T>;

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    function_: ScheduleEndpointFunction<T>
  ) {
    this.#endpoint = endpoint;
    this.#function = function_;
    this.#collector = new FunctionCollector((context, value) => stream.consume(context, value));
  }

  public endpoint(): DataSourceEndpoint {
    return this.#endpoint;
  }

  public consume(
    context: MessageContext,
    trigger: ScheduleTrigger
  ): ReturnType<Consumer<ScheduleTrigger>["consume"]> {
    return this.#function.onTrigger(context, trigger, this.#collector);
  }
}

class CronEndpoint extends DataSourceEndpoint {
  #binding: CronEndpointBinding | undefined;
  #job: Cron | undefined;
  #timer: NodeJS.Timeout | undefined;
  #running = false;
  #generation = 0;
  readonly #active = new Set<Promise<void>>();

  public bind(binding: CronEndpointBinding): void {
    if (this.#binding !== undefined) {
      throw new Error(`consumer already assigned to cron endpoint ${this.name}`);
    }
    this.#binding = binding;
    this.addEndpointConsumer(binding);
  }

  public start(): void {
    if (this.#job !== undefined) throw new Error(`cron endpoint ${this.name} is already started`);
    const config = this.cronConfig();
    if (!config.enabled) return;
    const binding = this.#binding;
    if (binding === undefined) throw new Error(`cron endpoint ${this.name} has no consumer`);
    const job = new Cron(config.schedule, {
      name: `${this.dataSource().name}.${this.name}`,
      paused: true,
      timezone: config.timezone
    });
    this.#job = job;
    const next = job.nextRun();
    if (next === null) throw new Error(`cron endpoint ${this.name} has no next occurrence`);
    this.#schedule(next, binding, config, this.#generation);
  }

  public async stop(): Promise<void> {
    this.#generation += 1;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#job?.stop();
    this.#job = undefined;
    await Promise.allSettled(this.#active);
  }

  #schedule(
    next: Date,
    binding: CronEndpointBinding,
    config: CronEndpointConfig,
    generation: number
  ): void {
    const delay = Math.min(Math.max(next.getTime() - Date.now(), 0), 30_000);
    this.#timer = setTimeout(() => {
      if (generation !== this.#generation || this.#job === undefined) return;
      const now = new Date();
      if (now < next) {
        this.#schedule(next, binding, config, generation);
        return;
      }

      const due: Date[] = [];
      let candidate: Date | null = next;
      while (candidate !== null && candidate <= now) {
        // Croner remains the sole parser/evaluator. match() rejects a shifted
        // spring-gap candidate; Croner itself emits only the first fall fold.
        if (this.#job.match(candidate)) due.push(candidate);
        candidate = this.#job.nextRun(candidate);
      }
      if (due.length === 1) {
        for (const occurrence of due) {
          this.#startDispatch(binding, occurrence, config);
        }
      } else if (due.length > 1 && config.missedRunPolicy === "FireOnce") {
        const occurrence = due.at(-1);
        if (occurrence !== undefined) this.#startDispatch(binding, occurrence, config);
      }
      if (candidate !== null) this.#schedule(candidate, binding, config, generation);
    }, delay);
  }

  #startDispatch(
    binding: CronEndpointBinding,
    scheduledAt: Date,
    config: CronEndpointConfig
  ): void {
    const execution = this.#dispatch(binding, scheduledAt, config);
    this.#active.add(execution);
    void execution.finally(() => {
      this.#active.delete(execution);
    });
  }

  async #dispatch(
    binding: CronEndpointBinding,
    scheduledAt: Date,
    config: CronEndpointConfig
  ): Promise<void> {
    if (this.#running && config.overlapPolicy === "Skip") return;
    this.#running = true;
    const context = applyDataSourceEndpointTracing(
      new MessageContext().withStreamId(newStreamId()),
      this.runtimeEnvironment(),
      this.id
    );
    const started = this.onRequestStart(context);
    let failure: Error | undefined;
    try {
      const trigger = makeScheduleTrigger(
        this.id,
        this.name,
        scheduledAt.toISOString(),
        new Date().toISOString(),
        ScheduleBackend.Local
      );
      await binding.consume(context, trigger);
    } catch (error: unknown) {
      failure = error instanceof Error ? error : new Error(String(error));
      this.runtimeEnvironment()
        .log()
        .error(
          Context.background(),
          "cron endpoint execution failed",
          str("endpoint", this.name),
          err(failure)
        );
    } finally {
      this.#running = false;
      this.onRequestEnd(context, started, failure);
    }
  }

  private cronConfig(): CronEndpointConfig {
    const config = this.config();
    if (!isCronEndpointConfig(config)) {
      throw new Error(`endpoint ${this.name} is not a local cron endpoint`);
    }
    return config;
  }
}

function isCronEndpointConfig(config: EndpointConfig): config is CronEndpointConfig {
  return (
    "schedule" in config &&
    !("taskQueue" in config) &&
    typeof config.schedule === "string" &&
    "enabled" in config &&
    typeof config.enabled === "boolean" &&
    "timezone" in config &&
    typeof config.timezone === "string" &&
    "overlapPolicy" in config &&
    (config.overlapPolicy === "Allow" || config.overlapPolicy === "Skip") &&
    "missedRunPolicy" in config &&
    (config.missedRunPolicy === "FireOnce" || config.missedRunPolicy === "Skip")
  );
}

export class CronDataSource extends InputDataSource {
  #started = false;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    if (this.config().type !== DataConnectorType.Cron) {
      throw new Error(`data source ${this.name} is not cron`);
    }
  }

  public start(_context: Context): Promise<void> {
    void _context;
    if (this.#started)
      return Promise.reject(new Error(`cron data source ${this.name} is already started`));
    this.#started = true;
    try {
      for (const endpoint of this.cronEndpoints()) endpoint.start();
      return Promise.resolve();
    } catch (error: unknown) {
      this.#started = false;
      for (const endpoint of this.cronEndpoints()) void endpoint.stop();
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }

  public stop(_context: Context): Promise<void> {
    void _context;
    if (!this.#started) return Promise.resolve();
    this.#started = false;
    return Promise.all(this.cronEndpoints().map(async (endpoint) => endpoint.stop())).then(
      () => undefined
    );
  }

  private cronEndpoints(): readonly CronEndpoint[] {
    return this.endpoints().map((endpoint) => {
      if (!(endpoint instanceof CronEndpoint)) {
        throw new Error(`source endpoint ${endpoint.name} is not cron`);
      }
      return endpoint;
    });
  }
}

export function makeCronEndpointConsumer<T, R, E>(
  stream: TypedInputStream<T, R, E>,
  function_: ScheduleEndpointFunction<T>
): Consumer<ScheduleTrigger> {
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

function getOrCreateCronDataSource(
  connectorId: number,
  environment: RuntimeEnvironment
): CronDataSource {
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
