import type { DataConnectorConfig, EndpointConfig } from "./config/index.js";
import type { Context } from "./context.js";
import type { RuntimeEnvironment } from "./environment/index.js";
import type { AdmissionLifecycle } from "./lifecycle.js";
export interface DataConnector {
    readonly id: number;
    readonly name: string;
}
/** Shared client/worker lifecycle owned by a configured data connector. */
export interface ManagedDataConnector extends DataConnector, AdmissionLifecycle {
    startAdmission(context: Context): Promise<void>;
}
export interface Endpoint {
    readonly id: number;
    readonly name: string;
    dataConnector(): DataConnector;
    config(): EndpointConfig;
    runtimeEnvironment(): RuntimeEnvironment;
}
export declare abstract class RuntimeDataConnector implements DataConnector {
    #private;
    readonly id: number;
    readonly name: string;
    protected constructor(connectorId: number, environment: RuntimeEnvironment);
    config(): DataConnectorConfig;
    runtimeEnvironment(): RuntimeEnvironment;
}
//# sourceMappingURL=data-connector.d.ts.map