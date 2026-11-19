import { Kysely, PostgresDialect, sql } from "kysely";
import { Client, Pool } from "pg";
import Cursor from "pg-cursor";
import { noopLogger } from "../../runtime/environment/log.js";
import { requirePostgreSQLKyselyDataConnectorConfig } from "../../runtime/config/query.js";
/** One native pg pool per connector; no request-specific pools or background queue. */
export class Connector {
    id;
    name;
    db;
    operationTimeoutMs;
    #pool;
    #lifecycleContext;
    /** A supplied pool transfers ownership to this connector; stop ends it. */
    constructor(config, logger = noopLogger, pool) {
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
        this.db = new Kysely({ dialect: new PostgresDialect({ pool: this.#pool, cursor: Cursor, controlClient: Client }) });
    }
    async start(context) {
        context.signal().throwIfAborted();
        this.#lifecycleContext = context;
        // Startup uses the same pool; no schema changes are made implicitly.
        await sql `select 1`.execute(this.db, { signal: context.signal(), inflightQueryAbortStrategy: "cancel query" });
        context.signal().throwIfAborted();
    }
    async startAdmission(_context) { }
    async stopAdmission(_context) { }
    async stop(_context) {
        // This connector is the sole pool owner, including before Kysely initializes.
        await this.#pool.end();
    }
}
export function makeConnector(environment, config) {
    const existing = environment.managedDataConnectorById(config.id);
    if (existing !== undefined) {
        if (!(existing instanceof Connector))
            throw new Error(`connector ${String(config.id)} already has another runtime type`);
        return existing;
    }
    const connector = new Connector(config, environment.log());
    environment.addManagedDataConnector(connector);
    return connector;
}
//# sourceMappingURL=connector.js.map