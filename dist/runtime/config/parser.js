function record(value, path) {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error(`${path} must be a mapping`);
    }
    return value;
}
function stringValue(value, path) {
    if (typeof value !== "string") {
        throw new Error(`${path} must be a string`);
    }
    return value;
}
function optionalString(value, path) {
    return value === undefined ? undefined : stringValue(value, path);
}
function numberValue(value, path) {
    if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new Error(`${path} must be a finite number`);
    }
    return value;
}
function integer(value, path) {
    const result = numberValue(value, path);
    if (!Number.isSafeInteger(result)) {
        throw new Error(`${path} must be a safe integer`);
    }
    return result;
}
function optionalInteger(value, path) {
    return value === undefined ? undefined : integer(value, path);
}
function booleanValue(value, path) {
    if (typeof value !== "boolean") {
        throw new Error(`${path} must be a boolean`);
    }
    return value;
}
function optionalBoolean(value, path) {
    return value === undefined ? undefined : booleanValue(value, path);
}
function optionalEnum(value, path, allowed) {
    if (value === undefined)
        return undefined;
    const result = stringValue(value, path);
    if (!allowed.includes(result)) {
        throw new Error(`${path} must be one of: ${allowed.join(", ")}`);
    }
    return result;
}
function integerArray(value, path) {
    if (value === undefined) {
        return [];
    }
    if (!Array.isArray(value)) {
        throw new Error(`${path} must be a sequence`);
    }
    return value.map((item, index) => integer(item, `${path}[${String(index)}]`));
}
function properties(source, known) {
    return Object.fromEntries(Object.entries(source).filter(([key]) => !known.has(key)));
}
const identityKeys = new Set(["id", "name"]);
function identity(source, path) {
    return { id: integer(source.id, `${path}.id`), name: stringValue(source.name, `${path}.name`) };
}
function namedSection(root, section, parse) {
    const values = record(root[section] ?? {}, section);
    return Object.keys(values)
        .sort()
        .map((name) => parse(record(values[name], `${section}.${name}`), `${section}.${name}`));
}
const serviceKeys = new Set([
    ...identityKeys,
    "color",
    "defaultCallSemantics",
    "defaultGrpcTimeout",
    "environment",
    "golangVersion",
    "grpcHost",
    "grpcPort",
    "httpHost",
    "httpPort",
    "logLevel",
    "metricsHandler",
    "startupHandler",
    "readinessHandler",
    "livenessHandler",
    "kubernetesWorkloadType",
    "modulePath",
    "shutdownTimeout",
    "statusHandler"
]);
function callSemantics(value, path) {
    if (value === undefined || value === 0 || value === 1) {
        return undefined;
    }
    if (typeof value === "number") {
        switch (value) {
            case 2:
                return { functionCall: { async: false } };
            case 3:
                return { taskPool: { poolName: "" } };
            case 4:
                return { priorityTaskPool: { poolName: "", priority: 0 } };
            case 5:
                return { parallelCall: {} };
            default:
                throw new Error(`${path} has an unknown call semantics value`);
        }
    }
    const source = record(value, path);
    if ("functionCall" in source) {
        const config = record(source.functionCall, `${path}.functionCall`);
        return {
            functionCall: { async: optionalBoolean(config.async, `${path}.functionCall.async`) ?? false }
        };
    }
    if ("taskPool" in source) {
        const config = record(source.taskPool, `${path}.taskPool`);
        return { taskPool: { poolName: stringValue(config.poolName, `${path}.taskPool.poolName`) } };
    }
    if ("priorityTaskPool" in source) {
        const config = record(source.priorityTaskPool, `${path}.priorityTaskPool`);
        return {
            priorityTaskPool: {
                poolName: stringValue(config.poolName, `${path}.priorityTaskPool.poolName`),
                priority: integer(config.priority, `${path}.priorityTaskPool.priority`)
            }
        };
    }
    if ("parallelCall" in source) {
        record(source.parallelCall, `${path}.parallelCall`);
        return { parallelCall: {} };
    }
    throw new Error(`${path} must select exactly one call semantics`);
}
function parseService(source, path) {
    const kubernetesWorkloadType = stringValue(source.kubernetesWorkloadType, `${path}.kubernetesWorkloadType`);
    if (kubernetesWorkloadType !== "Deployment" && kubernetesWorkloadType !== "StatefulSet") {
        throw new Error(`${path}.kubernetesWorkloadType must be Deployment or StatefulSet`);
    }
    return {
        ...identity(source, path),
        color: stringValue(source.color, `${path}.color`),
        defaultCallSemantics: callSemantics(source.defaultCallSemantics, `${path}.defaultCallSemantics`),
        defaultGrpcTimeout: optionalInteger(source.defaultGrpcTimeout, `${path}.defaultGrpcTimeout`),
        environment: stringValue(source.environment, `${path}.environment`),
        golangVersion: optionalString(source.golangVersion, `${path}.golangVersion`),
        grpcHost: stringValue(source.grpcHost, `${path}.grpcHost`),
        grpcPort: integer(source.grpcPort, `${path}.grpcPort`),
        httpHost: stringValue(source.httpHost, `${path}.httpHost`),
        httpPort: integer(source.httpPort, `${path}.httpPort`),
        logLevel: optionalString(source.logLevel, `${path}.logLevel`),
        metricsHandler: stringValue(source.metricsHandler, `${path}.metricsHandler`),
        startupHandler: stringValue(source.startupHandler, `${path}.startupHandler`),
        readinessHandler: stringValue(source.readinessHandler, `${path}.readinessHandler`),
        livenessHandler: stringValue(source.livenessHandler, `${path}.livenessHandler`),
        kubernetesWorkloadType,
        modulePath: optionalString(source.modulePath, `${path}.modulePath`),
        shutdownTimeout: integer(source.shutdownTimeout, `${path}.shutdownTimeout`),
        statusHandler: stringValue(source.statusHandler, `${path}.statusHandler`),
        properties: properties(source, serviceKeys)
    };
}
const transformations = {
    1: "Input",
    2: "Map",
    3: "Filter",
    4: "Join",
    5: "MultiJoin",
    6: "Process",
    7: "FlatMap",
    8: "FlatMapIterable",
    9: "KeyBy",
    10: "Merge",
    11: "Split",
    12: "Case",
    13: "Sink",
    14: "CycleLink",
    15: "Error",
    16: "Delay",
    17: "When"
};
function transformation(value, path) {
    if (typeof value === "number") {
        const result = transformations[value];
        if (result !== undefined)
            return result;
    }
    if (typeof value === "string" &&
        Object.values(transformations).includes(value)) {
        return value;
    }
    throw new Error(`${path} has an unknown transformation type`);
}
function joinType(value, path) {
    const result = integer(value, path);
    if (result === 0 || result === 1 || result === 2 || result === 3 || result === 4) {
        return result;
    }
    throw new Error(`${path} has an unknown join type`);
}
function joinStorage(value, path) {
    const result = integer(value, path);
    if (result === 0 || result === 1 || result === 2 || result === 3) {
        return result;
    }
    throw new Error(`${path} has an unknown join storage type`);
}
const streamKeys = new Set([
    ...identityKeys,
    "type",
    "pipeline",
    "idService",
    "idSource",
    "idSources",
    "xPos",
    "yPos",
    "functionPackage",
    "functionName",
    "publicFunction",
    "functionDescription",
    "functionInitializerGroup",
    "functionModule",
    "valueType",
    "keyType",
    "idEndpoint",
    "joinType",
    "joinStorage",
    "ttl",
    "renewTTL",
    "pattern",
    "duration"
]);
function functionFields(source, path) {
    return {
        functionPackage: optionalString(source.functionPackage, `${path}.functionPackage`),
        functionName: optionalString(source.functionName, `${path}.functionName`),
        publicFunction: optionalBoolean(source.publicFunction, `${path}.publicFunction`),
        functionDescription: optionalString(source.functionDescription, `${path}.functionDescription`),
        functionInitializerGroup: optionalString(source.functionInitializerGroup, `${path}.functionInitializerGroup`),
        functionModule: optionalString(source.functionModule, `${path}.functionModule`)
    };
}
function parseStream(source, path) {
    const type = transformation(source.type, `${path}.type`);
    const common = {
        ...identity(source, path),
        type,
        pipeline: stringValue(source.pipeline, `${path}.pipeline`),
        idService: integer(source.idService, `${path}.idService`),
        idSource: optionalInteger(source.idSource, `${path}.idSource`) ?? 0,
        idSources: integerArray(source.idSources, `${path}.idSources`),
        xPos: numberValue(source.xPos, `${path}.xPos`),
        yPos: numberValue(source.yPos, `${path}.yPos`),
        properties: properties(source, streamKeys)
    };
    const functions = functionFields(source, path);
    switch (type) {
        case "Input":
            return {
                ...common,
                type,
                valueType: stringValue(source.valueType, `${path}.valueType`),
                idEndpoint: integer(source.idEndpoint, `${path}.idEndpoint`)
            };
        case "Map":
        case "FlatMap":
            return {
                ...common,
                ...functions,
                type,
                valueType: stringValue(source.valueType, `${path}.valueType`)
            };
        case "Filter":
        case "Case":
            return { ...common, ...functions, type };
        case "Delay":
            return {
                ...common,
                ...functions,
                type,
                duration: integer(source.duration, `${path}.duration`)
            };
        case "FlatMapIterable":
        case "When":
            return { ...common, type, valueType: stringValue(source.valueType, `${path}.valueType`) };
        case "KeyBy":
            return {
                ...common,
                ...functions,
                type,
                keyType: stringValue(source.keyType, `${path}.keyType`),
                valueType: stringValue(source.valueType, `${path}.valueType`)
            };
        case "Join":
            return {
                ...common,
                ...functions,
                type,
                valueType: stringValue(source.valueType, `${path}.valueType`),
                joinType: joinType(source.joinType, `${path}.joinType`),
                joinStorage: joinStorage(source.joinStorage, `${path}.joinStorage`),
                ttl: integer(source.ttl, `${path}.ttl`),
                renewTTL: booleanValue(source.renewTTL, `${path}.renewTTL`)
            };
        case "MultiJoin":
            return {
                ...common,
                ...functions,
                type,
                valueType: stringValue(source.valueType, `${path}.valueType`),
                joinStorage: joinStorage(source.joinStorage, `${path}.joinStorage`),
                ttl: integer(source.ttl, `${path}.ttl`),
                renewTTL: booleanValue(source.renewTTL, `${path}.renewTTL`)
            };
        case "Process":
            return {
                ...common,
                ...functions,
                type,
                pattern: optionalString(source.pattern, `${path}.pattern`)
            };
        case "Sink":
            return {
                ...common,
                type,
                valueType: optionalString(source.valueType, `${path}.valueType`),
                idEndpoint: integer(source.idEndpoint, `${path}.idEndpoint`)
            };
        case "CycleLink":
            return { ...common, type };
        case "Merge":
        case "Split":
            return { ...common, type };
        case "Error":
            throw new Error(`${path}.type Error is a virtual runtime stream, not a config stream`);
    }
}
const connectorKeys = new Set([
    ...identityKeys,
    "type",
    "implementation",
    "programmingLanguage",
    "module",
    "host",
    "port",
    "useDedicatedListener",
    "address",
    "connectionsCount",
    "brokers",
    "version",
    "dialTimeout",
    "usePartitioner",
    "async",
    "securityProtocol",
    "saslMechanism",
    "username",
    "password",
    "namespace",
    "identity",
    "apiKey",
    "tlsEnabled",
    "tlsServerName",
    "tlsCaFile",
    "tlsCertFile",
    "tlsKeyFile",
    "maxConcurrentActivities",
    "maxConcurrentWorkflows"
]);
function parseConnector(source, path) {
    const type = integer(source.type, `${path}.type`);
    const common = {
        ...identity(source, path),
        type,
        implementation: stringValue(source.implementation, `${path}.implementation`),
        properties: properties(source, connectorKeys)
    };
    switch (type) {
        case 1:
            return {
                ...common,
                type,
                module: optionalString(source.module, `${path}.module`),
                host: optionalString(source.host, `${path}.host`),
                port: optionalInteger(source.port, `${path}.port`),
                useDedicatedListener: optionalBoolean(source.useDedicatedListener, `${path}.useDedicatedListener`) ?? false
            };
        case 2:
            return {
                ...common,
                type,
                programmingLanguage: optionalInteger(source.programmingLanguage, `${path}.programmingLanguage`),
                module: optionalString(source.module, `${path}.module`),
                address: optionalString(source.address, `${path}.address`),
                connectionsCount: optionalInteger(source.connectionsCount, `${path}.connectionsCount`) ?? 1
            };
        case 3:
            return {
                ...common,
                type,
                programmingLanguage: optionalInteger(source.programmingLanguage, `${path}.programmingLanguage`),
                brokers: optionalString(source.brokers, `${path}.brokers`) ?? "",
                version: optionalString(source.version, `${path}.version`),
                dialTimeout: source.dialTimeout === undefined
                    ? 0
                    : numberValue(source.dialTimeout, `${path}.dialTimeout`),
                usePartitioner: optionalBoolean(source.usePartitioner, `${path}.usePartitioner`) ?? false,
                async: optionalBoolean(source.async, `${path}.async`) ?? false,
                securityProtocol: optionalEnum(source.securityProtocol, `${path}.securityProtocol`, [
                    "PLAINTEXT",
                    "SASL_PLAINTEXT",
                    "SASL_SSL"
                ]) ?? "PLAINTEXT",
                saslMechanism: optionalEnum(source.saslMechanism, `${path}.saslMechanism`, [
                    "PLAIN",
                    "SCRAM-SHA-256",
                    "SCRAM-SHA-512"
                ]) ?? "PLAIN",
                username: optionalString(source.username, `${path}.username`),
                password: optionalString(source.password, `${path}.password`)
            };
        case 4:
            return { ...common, type: 4 };
        case 5:
            return { ...common, type: 5 };
        case 6:
            return {
                ...common,
                type: 6,
                address: optionalString(source.address, `${path}.address`) ?? "",
                namespace: optionalString(source.namespace, `${path}.namespace`) ?? "default",
                identity: optionalString(source.identity, `${path}.identity`) ?? "",
                apiKey: optionalString(source.apiKey, `${path}.apiKey`) ?? "",
                tlsEnabled: optionalBoolean(source.tlsEnabled, `${path}.tlsEnabled`) ?? false,
                tlsServerName: optionalString(source.tlsServerName, `${path}.tlsServerName`) ?? "",
                tlsCaFile: optionalString(source.tlsCaFile, `${path}.tlsCaFile`) ?? "",
                tlsCertFile: optionalString(source.tlsCertFile, `${path}.tlsCertFile`) ?? "",
                tlsKeyFile: optionalString(source.tlsKeyFile, `${path}.tlsKeyFile`) ?? "",
                maxConcurrentActivities: optionalInteger(source.maxConcurrentActivities, `${path}.maxConcurrentActivities`) ?? 0,
                maxConcurrentWorkflows: optionalInteger(source.maxConcurrentWorkflows, `${path}.maxConcurrentWorkflows`) ?? 0
            };
        default:
            throw new Error(`${path}.type has an unknown data connector type`);
    }
}
const endpointKeys = new Set([
    ...identityKeys,
    "idDataConnector",
    "enabled",
    "tracingEnabled",
    "httpMethodType",
    "path",
    "grpcMethodType",
    "methodName",
    "createTopic",
    "topic",
    "partitions",
    "consumerGroup",
    "replicationFactor",
    "schedule",
    "scheduleId",
    "timezone",
    "overlapPolicy",
    "missedRunPolicy",
    "taskQueue",
    "workflowExecutionTimeout",
    "activityStartToCloseTimeout",
    "activityHeartbeatTimeout",
    "maximumAttempts",
    "functionPackage",
    "functionName",
    "publicFunction",
    "functionDescription",
    "functionInitializerGroup",
    "functionModule"
]);
function grpcMethod(value, path) {
    const values = {
        1: "NoStreaming",
        2: "ClientStreaming",
        4: "ServerStreaming",
        5: "BidirectionalStreaming"
    };
    if (typeof value === "number" && value in values)
        return values[value];
    if (typeof value === "string" && Object.values(values).includes(value))
        return value;
    throw new Error(`${path} has an unknown gRPC method type`);
}
function parseEndpoint(source, path) {
    const common = {
        ...identity(source, path),
        idDataConnector: integer(source.idDataConnector, `${path}.idDataConnector`),
        tracingEnabled: optionalBoolean(source["tracingEnabled"], `${path}.tracingEnabled`) ?? false,
        properties: properties(source, endpointKeys)
    };
    const functions = functionFields(source, path);
    if (source.httpMethodType !== undefined)
        return {
            ...common,
            ...functions,
            httpMethodType: stringValue(source.httpMethodType, `${path}.httpMethodType`),
            path: stringValue(source.path, `${path}.path`)
        };
    if (source.grpcMethodType !== undefined)
        return {
            ...common,
            ...functions,
            grpcMethodType: grpcMethod(source.grpcMethodType, `${path}.grpcMethodType`),
            methodName: stringValue(source.methodName, `${path}.methodName`)
        };
    if (source.topic !== undefined ||
        source.createTopic !== undefined ||
        source.consumerGroup !== undefined)
        return {
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            createTopic: optionalBoolean(source.createTopic, `${path}.createTopic`) ?? false,
            topic: optionalString(source.topic, `${path}.topic`) ?? "",
            partitions: optionalInteger(source.partitions, `${path}.partitions`) ?? 0,
            consumerGroup: optionalString(source.consumerGroup, `${path}.consumerGroup`) ?? "",
            replicationFactor: optionalInteger(source.replicationFactor, `${path}.replicationFactor`) ?? 0
        };
    if (source.taskQueue !== undefined || source.scheduleId !== undefined)
        return {
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            taskQueue: optionalString(source.taskQueue, `${path}.taskQueue`) ?? "",
            schedule: optionalString(source.schedule, `${path}.schedule`) ?? "",
            scheduleId: optionalString(source.scheduleId, `${path}.scheduleId`) ?? "",
            timezone: optionalString(source.timezone, `${path}.timezone`) ?? "UTC",
            overlapPolicy: optionalEnum(source.overlapPolicy, `${path}.overlapPolicy`, ["Allow", "Skip"]) ??
                "Skip",
            missedRunPolicy: optionalEnum(source.missedRunPolicy, `${path}.missedRunPolicy`, [
                "FireOnce",
                "Skip"
            ]) ?? "Skip",
            workflowExecutionTimeout: optionalInteger(source.workflowExecutionTimeout, `${path}.workflowExecutionTimeout`) ?? 0,
            activityStartToCloseTimeout: optionalInteger(source.activityStartToCloseTimeout, `${path}.activityStartToCloseTimeout`) ?? 0,
            activityHeartbeatTimeout: optionalInteger(source.activityHeartbeatTimeout, `${path}.activityHeartbeatTimeout`) ?? 0,
            maximumAttempts: optionalInteger(source.maximumAttempts, `${path}.maximumAttempts`) ?? 0
        };
    if (source.schedule !== undefined)
        return {
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            schedule: stringValue(source.schedule, `${path}.schedule`),
            timezone: optionalString(source.timezone, `${path}.timezone`) ?? "UTC",
            overlapPolicy: optionalEnum(source.overlapPolicy, `${path}.overlapPolicy`, ["Allow", "Skip"]) ??
                "Skip",
            missedRunPolicy: optionalEnum(source.missedRunPolicy, `${path}.missedRunPolicy`, [
                "FireOnce",
                "Skip"
            ]) ?? "Skip"
        };
    return { ...common, ...functions };
}
const linkKeys = new Set(["from", "to", "callSemantics", "poolName", "priority", "async"]);
function parseLink(source, path) {
    const semantics = callSemantics(source.callSemantics, `${path}.callSemantics`);
    let effective = semantics;
    if (semantics !== undefined && "functionCall" in semantics && source.async !== undefined)
        effective = { functionCall: { async: booleanValue(source.async, `${path}.async`) } };
    if (semantics !== undefined && "taskPool" in semantics && source.poolName !== undefined)
        effective = { taskPool: { poolName: stringValue(source.poolName, `${path}.poolName`) } };
    if (semantics !== undefined && "priorityTaskPool" in semantics)
        effective = {
            priorityTaskPool: {
                poolName: stringValue(source.poolName, `${path}.poolName`),
                priority: integer(source.priority, `${path}.priority`)
            }
        };
    return {
        from: integer(source.from, `${path}.from`),
        to: integer(source.to, `${path}.to`),
        callSemantics: effective,
        properties: properties(source, linkKeys)
    };
}
function parsePool(source, path) {
    const keys = new Set(["name", "executorsCount", "queueCapacity"]);
    return {
        name: stringValue(source.name, `${path}.name`),
        executorsCount: integer(source.executorsCount, `${path}.executorsCount`),
        queueCapacity: optionalInteger(source.queueCapacity, `${path}.queueCapacity`) ?? 0,
        properties: properties(source, keys)
    };
}
function parseModule(source, path) {
    const keys = new Set(["name", "path"]);
    return {
        name: stringValue(source.name, `${path}.name`),
        path: optionalString(source.path, `${path}.path`) ?? "",
        properties: properties(source, keys)
    };
}
function parseType(source, path) {
    const keys = new Set([
        "name",
        "type",
        "typeDefinition",
        "typeImport",
        "valueType",
        "keyType",
        "package",
        "module",
        "definitionFormat",
        "publicType",
        "transferByValue",
        "useAlias"
    ]);
    return {
        name: stringValue(source.name, `${path}.name`),
        type: stringValue(source.type, `${path}.type`),
        typeDefinition: optionalString(source.typeDefinition, `${path}.typeDefinition`),
        typeImport: optionalString(source.typeImport, `${path}.typeImport`),
        valueType: optionalString(source.valueType, `${path}.valueType`),
        keyType: optionalString(source.keyType, `${path}.keyType`),
        package: optionalString(source.package, `${path}.package`),
        module: optionalString(source.module, `${path}.module`),
        definitionFormat: optionalInteger(source.definitionFormat, `${path}.definitionFormat`),
        publicType: optionalBoolean(source.publicType, `${path}.publicType`) ?? false,
        transferByValue: optionalBoolean(source.transferByValue, `${path}.transferByValue`) ?? false,
        useAlias: optionalBoolean(source.useAlias, `${path}.useAlias`) ?? false,
        properties: properties(source, keys)
    };
}
const rootKeys = new Set([
    "settings",
    "services",
    "streams",
    "dataConnectors",
    "endpoints",
    "pools",
    "links",
    "modules",
    "types"
]);
export function parseCanonicalConfig(value) {
    const root = record(value, "config");
    return {
        services: namedSection(root, "services", parseService),
        streams: namedSection(root, "streams", parseStream),
        dataConnectors: namedSection(root, "dataConnectors", parseConnector),
        endpoints: namedSection(root, "endpoints", parseEndpoint),
        pools: namedSection(root, "pools", parsePool),
        links: namedSection(root, "links", parseLink),
        modules: namedSection(root, "modules", parseModule),
        types: namedSection(root, "types", parseType),
        properties: properties(root, rootKeys)
    };
}
export const canonicalConfigSchema = { parse: parseCanonicalConfig };
//# sourceMappingURL=parser.js.map