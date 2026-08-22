import { deepFreeze } from "./immutable.js";
function indexIdentities(category, values) {
    const byId = new Map();
    const byName = new Map();
    for (const value of values) {
        if (byName.has(value.name)) {
            throw new Error(`duplicate ${category} name: ${value.name}`);
        }
        if (byId.has(value.id)) {
            throw new Error(`duplicate ${category} id: ${String(value.id)}`);
        }
        byName.set(value.name, value);
        byId.set(value.id, value);
    }
    return { byId, byName };
}
function linkKey(from, to) {
    return `${String(from)}:${String(to)}`;
}
export class RuntimeConfig {
    #config;
    #services;
    #streams;
    #connectors;
    #endpoints;
    #pools = new Map();
    #modules = new Map();
    #types = new Map();
    #links = new Map();
    constructor(config) {
        this.#config = deepFreeze(config);
        this.#services = indexIdentities("service", this.#config.services);
        this.#streams = indexIdentities("stream", this.#config.streams);
        this.#connectors = indexIdentities("data connector", this.#config.dataConnectors);
        this.#endpoints = indexIdentities("endpoint", this.#config.endpoints);
        this.indexNamed("pool", this.#config.pools, this.#pools);
        this.indexNamed("module", this.#config.modules, this.#modules);
        this.indexNamed("type", this.#config.types, this.#types);
        this.indexLinks(this.#config.links);
        this.validateReferences();
    }
    config() {
        return this.#config;
    }
    serviceByName(name) {
        return this.#services.byName.get(name);
    }
    serviceById(id) {
        return this.#services.byId.get(id);
    }
    streamByName(name) {
        return this.#streams.byName.get(name);
    }
    streamById(id) {
        return this.#streams.byId.get(id);
    }
    dataConnectorById(id) {
        return this.#connectors.byId.get(id);
    }
    endpointById(id) {
        return this.#endpoints.byId.get(id);
    }
    poolByName(name) {
        return this.#pools.get(name);
    }
    moduleByName(name) {
        return this.#modules.get(name);
    }
    typeByName(name) {
        return this.#types.get(name);
    }
    link(from, to) {
        return this.#links.get(linkKey(from, to));
    }
    indexNamed(category, values, target) {
        for (const value of values) {
            if (target.has(value.name)) {
                throw new Error(`duplicate ${category} name: ${value.name}`);
            }
            target.set(value.name, value);
        }
    }
    indexLinks(links) {
        for (const link of links) {
            const key = linkKey(link.from, link.to);
            if (this.#links.has(key)) {
                throw new Error(`duplicate link from=${String(link.from)} to=${String(link.to)}`);
            }
            this.#links.set(key, link);
        }
    }
    validateReferences() {
        for (const service of this.#config.services) {
            validatePort(service.grpcPort, `service ${service.name} grpcPort`);
            validatePort(service.httpPort, `service ${service.name} httpPort`);
            validateNonNegative(service.shutdownTimeout, `service ${service.name} shutdownTimeout`);
            if (service.defaultGrpcTimeout !== undefined) {
                validateNonNegative(service.defaultGrpcTimeout, `service ${service.name} defaultGrpcTimeout`);
            }
            this.validateCallSemantics(service.defaultCallSemantics, `service ${service.name}`);
        }
        for (const pool of this.#config.pools) {
            validatePositive(pool.executorsCount, `pool ${pool.name} executorsCount`);
            validateNonNegative(pool.queueCapacity, `pool ${pool.name} queueCapacity`);
        }
        for (const connector of this.#config.dataConnectors) {
            if (connector.type === 2) {
                const connectionsCount = "connectionsCount" in connector ? connector.connectionsCount : undefined;
                if (typeof connectionsCount !== "number") {
                    throw new RangeError(`gRPC data connector ${connector.name} connectionsCount must be a positive integer`);
                }
                validatePositive(connectionsCount, `gRPC data connector ${connector.name} connectionsCount`);
            }
        }
        for (const stream of this.#config.streams) {
            if (!this.#services.byId.has(stream.idService)) {
                throw new Error(`stream ${stream.name} references missing service id ${String(stream.idService)}`);
            }
            for (const sourceId of [stream.idSource, ...stream.idSources]) {
                if (sourceId !== 0 && !this.#streams.byId.has(sourceId)) {
                    throw new Error(`stream ${stream.name} references missing source stream id ${String(sourceId)}`);
                }
            }
            if ((stream.type === "Input" || stream.type === "Sink") && "idEndpoint" in stream) {
                const endpointId = stream.idEndpoint;
                if (typeof endpointId !== "number" || !this.#endpoints.byId.has(endpointId)) {
                    throw new Error(`${stream.type} stream ${stream.name} references missing endpoint id ${String(endpointId)}`);
                }
            }
        }
        for (const endpoint of this.#config.endpoints) {
            const connector = this.#connectors.byId.get(endpoint.idDataConnector);
            if (connector === undefined) {
                throw new Error(`endpoint ${endpoint.name} references missing data connector id ${String(endpoint.idDataConnector)}`);
            }
            const endpointType = "httpMethodType" in endpoint
                ? 1
                : "grpcMethodType" in endpoint
                    ? 2
                    : "topic" in endpoint
                        ? 3
                        : 4;
            if (connector.type !== endpointType) {
                throw new Error(`endpoint ${endpoint.name} type does not match data connector ${connector.name}`);
            }
        }
        for (const link of this.#config.links) {
            if (!this.#streams.byId.has(link.from) || !this.#streams.byId.has(link.to)) {
                throw new Error(`link from=${String(link.from)} to=${String(link.to)} references missing stream`);
            }
            this.validateCallSemantics(link.callSemantics, `link from=${String(link.from)} to=${String(link.to)}`);
        }
    }
    validateCallSemantics(semantics, owner) {
        if (semantics === undefined || "functionCall" in semantics || "parallelCall" in semantics) {
            return;
        }
        const poolName = "taskPool" in semantics ? semantics.taskPool.poolName : semantics.priorityTaskPool.poolName;
        if (poolName !== "" && !this.#pools.has(poolName)) {
            throw new Error(`${owner} references missing pool ${poolName}`);
        }
    }
}
function validatePort(value, path) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 65_535) {
        throw new RangeError(`${path} must be an integer between 0 and 65535`);
    }
}
function validateNonNegative(value, path) {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new RangeError(`${path} must be a non-negative integer`);
    }
}
function validatePositive(value, path) {
    if (!Number.isSafeInteger(value) || value < 1) {
        throw new RangeError(`${path} must be a positive integer`);
    }
}
//# sourceMappingURL=runtime-config.js.map