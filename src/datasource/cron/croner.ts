import { Cron } from "croner";

import {
  type Context,
  ContextDeadlineExceededError,
  DataConnectorType,
  DataSourceEndpoint,
  FunctionCollector,
  InputDataSource,
  MessageContext,
  RuntimeDrainTimeoutError,
  ScheduleBackend,
  applyDataSourceEndpointTracing,
  err,
  makeScheduleTrigger,
  newStreamId,
  settleWithinDeadline,
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

interface PendingCronResult {
  readonly promise: Promise<undefined>;
  readonly resolve: (value: undefined) => void;
  settled: boolean;
}

class CronEndpointConsumer<T, R, E> implements InputEndpointConsumer, Consumer<ScheduleTrigger> {
  readonly #endpoint: DataSourceEndpoint;
  readonly #function: ScheduleEndpointFunction<T>;
  readonly #collector: FunctionCollector<T>;
  readonly #hasResult: boolean;
  readonly #pending = new Map<string, PendingCronResult>();

  public constructor(
    endpoint: DataSourceEndpoint,
    stream: TypedInputStream<T, R, E>,
    function_: ScheduleEndpointFunction<T>
  ) {
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

  public endpoint(): DataSourceEndpoint {
    return this.#endpoint;
  }

  public async consume(context: MessageContext, trigger: ScheduleTrigger): Promise<void> {
    if (!this.#hasResult) {
      await this.#function.onTrigger(context, trigger, this.#collector);
      return;
    }
    const streamId = context.streamId();
    if (streamId === undefined || streamId === "") {
      throw new Error(`cron endpoint ${this.#endpoint.name} activation has no stream ID`);
    }
    if (this.#pending.has(streamId)) {
      throw new Error(
        `cron endpoint ${this.#endpoint.name} already has an active execution ${streamId}`
      );
    }
    const resolution = Promise.withResolvers<undefined>();
    const pending: PendingCronResult = { ...resolution, settled: false };
    this.#pending.set(streamId, pending);
    this.#endpoint.onPendingAdd(context, streamId);
    try {
      await this.#function.onTrigger(context, trigger, this.#collector);
      await pending.promise;
    } finally {
      this.#pending.delete(streamId);
      this.#endpoint.onPendingRemove(context, streamId);
    }
  }

  private consumeResult(context: MessageContext): void {
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
  #binding: CronEndpointBinding | undefined;
  #job: Cron | undefined;
  #running = false;
  readonly #active = new Set<Promise<void>>();

  public bind(binding: CronEndpointBinding): void {
    if (this.#binding !== undefined) {
      throw new Error(`consumer already assigned to cron endpoint ${this.name}`);
    }
    this.#binding = binding;
    this.addEndpointConsumer(binding);
  }

  public start(context: Context): void {
    if (this.#job !== undefined || this.#active.size !== 0)
      throw new Error(`cron endpoint ${this.name} is already started`);
    const config = this.cronConfig();
    if (!config.enabled) return;
    const binding = this.#binding;
    if (binding === undefined) throw new Error(`cron endpoint ${this.name} has no consumer`);
    let nextScheduledAt: Date | null = null;
    const job = new Cron(
      config.schedule,
      {
        name: `${this.dataSource().name}.${this.name}`,
        timezone: config.timezone,
        protect: config.overlapPolicy === "Skip"
      },
      (current) => {
        const scheduledAt = nextScheduledAt ?? current.currentRun() ?? new Date();
        const next = current.nextRun(scheduledAt);
        nextScheduledAt = next;
        const currentConfig = this.cronConfig();
        if (
          currentConfig.missedRunPolicy === "Skip" &&
          next !== null &&
          next.getTime() <= Date.now()
        ) {
          return;
        }
        if (context.cancelled()) return;
        return this.#startDispatch(context, binding, scheduledAt);
      }
    );
    this.#job = job;
    nextScheduledAt = job.nextRun();
    if (nextScheduledAt === null) {
      job.stop();
      this.#job = undefined;
      throw new Error(`cron endpoint ${this.name} has no next occurrence`);
    }
  }

  public async stop(context: Context): Promise<void> {
    this.#job?.stop();
    this.#job = undefined;
    const remainingMs = context.remainingMs();
    const results = await settleWithinDeadline(context, [...this.#active]);
    if (results.some((result) => result === undefined)) {
      const reason: unknown = context.signal().reason;
      if (context.signal().aborted && !(reason instanceof ContextDeadlineExceededError)) {
        throw reason instanceof Error
          ? reason
          : new Error("cron shutdown cancelled", { cause: reason });
      }
      throw new RuntimeDrainTimeoutError(remainingMs ?? 0);
    }
    for (const result of results) {
      if (result?.status === "rejected") throw result.reason;
    }
  }

  #startDispatch(owner: Context, binding: CronEndpointBinding, scheduledAt: Date): Promise<void> {
    const execution = this.#dispatch(owner, binding, scheduledAt);
    this.#active.add(execution);
    return execution.finally(() => {
      this.#active.delete(execution);
    });
  }

  async #dispatch(owner: Context, binding: CronEndpointBinding, scheduledAt: Date): Promise<void> {
    const config = this.cronConfig();
    if (this.#running && config.overlapPolicy === "Skip") return;
    this.#running = true;
    const context = applyDataSourceEndpointTracing(
      (owner instanceof MessageContext ? owner : MessageContext.fromContext(owner)).withStreamId(
        newStreamId()
      ),
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
          context,
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
  #generation = 0;

  public constructor(connectorId: number, environment: RuntimeEnvironment) {
    super(connectorId, environment);
    if (this.config().type !== DataConnectorType.Cron) {
      throw new Error(`data source ${this.name} is not cron`);
    }
  }

  public async start(context: Context): Promise<void> {
    if (context.cancelled()) {
      const reason: unknown = context.signal().reason;
      return Promise.reject(
        reason instanceof Error
          ? reason
          : new Error("cron source context cancelled", { cause: reason })
      );
    }
    if (this.#started)
      return Promise.reject(new Error(`cron data source ${this.name} is already started`));
    this.#started = true;
    const generation = ++this.#generation;
    try {
      for (const endpoint of this.cronEndpoints()) endpoint.start(context);
      return;
    } catch (error: unknown) {
      const cleanup = await Promise.allSettled(
        this.cronEndpoints().map(async (endpoint) => endpoint.stop(context))
      );
      if (this.#generation === generation) {
        this.#started = cleanup.some((result) => result.status === "rejected");
      }
      throw error instanceof Error ? error : new Error(String(error));
    }
  }

  public async stop(context: Context): Promise<void> {
    if (!this.#started) return;
    const generation = this.#generation;
    await Promise.all(this.cronEndpoints().map(async (endpoint) => endpoint.stop(context)));
    if (this.#generation === generation) this.#started = false;
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
