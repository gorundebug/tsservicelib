import type { CanonicalConfig, DataConnectorConfig, EndpointConfig, LinkConfig, ModuleConfig, PoolConfig, ServiceConfig, StreamConfig, TypeConfig } from "./types.js";
export declare class RuntimeConfig<T extends CanonicalConfig = CanonicalConfig> {
    #private;
    constructor(config: T);
    config(): T;
    serviceByName(name: string): ServiceConfig | undefined;
    serviceById(id: number): ServiceConfig | undefined;
    streamByName(name: string): StreamConfig | undefined;
    streamById(id: number): StreamConfig | undefined;
    dataConnectorById(id: number): DataConnectorConfig | undefined;
    endpointById(id: number): EndpointConfig | undefined;
    poolByName(name: string): PoolConfig | undefined;
    moduleByName(name: string): ModuleConfig | undefined;
    typeByName(name: string): TypeConfig | undefined;
    link(from: number, to: number): LinkConfig | undefined;
    private indexNamed;
    private indexLinks;
    private validateReferences;
    private validateCallSemantics;
}
//# sourceMappingURL=runtime-config.d.ts.map