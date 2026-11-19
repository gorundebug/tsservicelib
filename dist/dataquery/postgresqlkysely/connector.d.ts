import { Kysely } from "kysely";
import { Pool } from "pg";
import { type Logger } from "../../runtime/environment/log.js";
import type { Context } from "../../runtime/context.js";
import type { ManagedDataConnector } from "../../runtime/data-connector.js";
import type { RuntimeEnvironment } from "../../runtime/environment/index.js";
import type { PostgreSQLKyselyDataConnectorConfig } from "../../runtime/config/types.js";
export type PostgreSQLKyselyConfig = PostgreSQLKyselyDataConnectorConfig;
/** One native pg pool per connector; no request-specific pools or background queue. */
export declare class Connector<DB> implements ManagedDataConnector {
    #private;
    readonly id: number;
    readonly name: string;
    readonly db: Kysely<DB>;
    readonly operationTimeoutMs: number;
    /** A supplied pool transfers ownership to this connector; stop ends it. */
    constructor(config: PostgreSQLKyselyConfig, logger?: Logger, pool?: Pool);
    start(context: Context): Promise<void>;
    startAdmission(_context: Context): Promise<void>;
    stopAdmission(_context: Context): Promise<void>;
    stop(_context: Context): Promise<void>;
}
export declare function makeConnector<DB>(environment: RuntimeEnvironment, config: PostgreSQLKyselyConfig): Connector<DB>;
//# sourceMappingURL=connector.d.ts.map