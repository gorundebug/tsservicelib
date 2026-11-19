import { Kysely, PostgresDialect, sql } from "kysely";
import { Client, Pool } from "pg";
import Cursor from "pg-cursor";
import { noopLogger, type Logger } from "../../runtime/environment/log.js";
import type { Context } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";

import type { PostgreSQLKyselyDataConnectorConfig } from "../../runtime/config/types.js";
import { requirePostgreSQLKyselyDataConnectorConfig } from "../../runtime/config/query.js";

export type PostgreSQLKyselyConfig = PostgreSQLKyselyDataConnectorConfig;

/** One native pg pool per connector; no request-specific pools or background queue. */
export class Connector<DB> implements ManagedDataConnector {
  public readonly id: number;
  public readonly name: string;
  public readonly db: Kysely<DB>;
  public readonly operationTimeoutMs: number;
  readonly #pool: Pool;
  #lifecycleContext: Context | undefined;

  /** A supplied pool transfers ownership to this connector; stop ends it. */
  public constructor(config: PostgreSQLKyselyConfig, logger: Logger = noopLogger, pool?: Pool) {
    requirePostgreSQLKyselyDataConnectorConfig(config);
    if (!Number.isSafeInteger(config.id) || config.id <= 0 ||
        !Number.isSafeInteger(config.connectionsCount) || config.connectionsCount <= 0 || !config.connectionString) {
      throw new Error("PostgreSQL Query requires id, connectionString and positive connectionsCount");
    }
    for (const timeout of [config.connectTimeoutMs, config.operationTimeoutMs]) {
      if (timeout !== undefined && (!Number.isSafeInteger(timeout) || timeout < 0)) {
        throw new Error("PostgreSQL Query timeouts must be nonnegative integers");
      }
    }
    this.id = config.id;
    this.name = config.name;
    this.operationTimeoutMs = config.operationTimeoutMs ?? 0;
    this.#pool = pool ?? new Pool({
      connectionString: config.connectionString,
      max: config.connectionsCount,
      connectionTimeoutMillis: config.connectTimeoutMs ?? 0,
      statement_timeout: config.operationTimeoutMs ?? 0
    });
    // pg removes failed idle clients itself; observe its required error event
    // through the service logger instead of allowing EventEmitter to terminate.
    this.#pool.on("error", () => {
      if (this.#lifecycleContext !== undefined) {
        logger.error(this.#lifecycleContext, "PostgreSQL idle connection failed", {
          key: "connector", type: "string", value: this.name
        });
      }
    });
    this.db = new Kysely<DB>({ dialect: new PostgresDialect({ pool: this.#pool, cursor: Cursor, controlClient: Client }) });
  }

  public async start(context: Context): Promise<void> {
    context.signal().throwIfAborted();
    this.#lifecycleContext = context;
    // Startup uses the same pool; no schema changes are made implicitly.
    await sql`select 1`.execute(this.db, { signal: context.signal(), inflightQueryAbortStrategy: "cancel query" });
    context.signal().throwIfAborted();
  }
  public async startAdmission(_context: Context): Promise<void> {}
  public async stopAdmission(_context: Context): Promise<void> {}
  public async stop(_context: Context): Promise<void> {
    // This connector is the sole pool owner, including before Kysely initializes.
    await this.#pool.end();
  }
}

export function makeConnector<DB>(environment: RuntimeEnvironment, config: PostgreSQLKyselyConfig): Connector<DB> {
  const existing = environment.managedDataConnectorById(config.id);
  if (existing !== undefined) {
    if (!(existing instanceof Connector)) throw new Error(`connector ${String(config.id)} already has another runtime type`);
    return existing as Connector<DB>;
  }
  const connector = new Connector<DB>(config, environment.log());
  environment.addManagedDataConnector(connector);
  return connector;
}
