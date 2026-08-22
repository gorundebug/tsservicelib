export class RuntimeDataConnector {
    #environment;
    id;
    name;
    constructor(connectorId, environment) {
        const config = environment.runtimeConfig().dataConnectorById(connectorId);
        if (config === undefined) {
            throw new Error(`data connector config ${String(connectorId)} not found`);
        }
        this.id = connectorId;
        this.name = config.name;
        this.#environment = environment;
    }
    config() {
        const config = this.#environment.runtimeConfig().dataConnectorById(this.id);
        if (config === undefined) {
            throw new Error(`data connector config ${String(this.id)} not found`);
        }
        return config;
    }
    runtimeEnvironment() {
        return this.#environment;
    }
}
//# sourceMappingURL=data-connector.js.map