import { requireS3AWSDataConnectorConfig, requirePostgreSQLKyselyDataConnectorConfig, requireQueryEndpointConfig } from "./query.js";
import { requirePulsarDataConnectorConfig, requirePulsarEndpointConfig } from "./pulsar.js";
import { requireRedisStreamsDataConnectorConfig, requireRedisStreamsEndpointConfig } from "./redis.js";
import { requireRabbitMQDataConnectorConfig, requireRabbitMQEndpointConfig } from "./rabbitmq.js";
import { requireNATSDataConnectorConfig } from "./nats.js";
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
function requiredEnum(value, path, allowed) {
    const result = optionalEnum(value, path, allowed);
    if (result === undefined)
        throw new Error(`${path} is required`);
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
    17: "When",
    19: "Query",
    18: "SubStream"
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
    "component",
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
        component: optionalString(source.component, `${path}.component`),
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
        case "SubStream":
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
        case "Query":
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
    "region", "forcePathStyle",
    "connectionString",
    "adminUrl",
    "adminToken",
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
    "workerStopTimeout",
    "servers",
    "connectTimeoutMs",
    "virtualHost",
    "redisDatabase",
    "operationTimeoutMs",
    "token",
    "credentialsFile"
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
        case 18:
            return requireS3AWSDataConnectorConfig({
                ...common, type: 18,
                region: stringValue(source["region"], `${path}.region`),
                address: optionalString(source.address, `${path}.address`),
                forcePathStyle: optionalBoolean(source["forcePathStyle"], `${path}.forcePathStyle`) ?? false
            });
        case 13:
            return requirePostgreSQLKyselyDataConnectorConfig({
                ...common, type: 13,
                connectionString: stringValue(source["connectionString"], `${path}.connectionString`),
                connectionsCount: optionalInteger(source.connectionsCount, `${path}.connectionsCount`) ?? 10,
                connectTimeoutMs: optionalInteger(source["connectTimeoutMs"], `${path}.connectTimeoutMs`) ?? 0,
                operationTimeoutMs: optionalInteger(source["operationTimeoutMs"], `${path}.operationTimeoutMs`) ?? 0
            });
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
                workerStopTimeout: optionalInteger(source["workerStopTimeout"], `${path}.workerStopTimeout`) ?? 0
            };
        case 11: {
            const servers = source["servers"];
            if (!Array.isArray(servers) || servers.some((v) => typeof v !== "string"))
                throw new Error(`${path}.servers requires a URL array`);
            for (const k of [
                "username",
                "password",
                "credentialsFile",
                "tlsEnabled",
                "tlsServerName",
                "brokers",
                "securityProtocol",
                "saslMechanism"
            ])
                if (source[k] !== undefined)
                    throw new Error(`${path}.${k} is not a Pulsar field`);
            return requirePulsarDataConnectorConfig({
                ...common,
                type: 11,
                servers: [...servers],
                adminUrl: optionalString(source["adminUrl"], `${path}.adminUrl`) ?? "",
                adminToken: optionalString(source["adminToken"], `${path}.adminToken`),
                connectTimeoutMs: optionalInteger(source["connectTimeoutMs"], `${path}.connectTimeoutMs`) ?? 2000,
                operationTimeoutMs: optionalInteger(source["operationTimeoutMs"], `${path}.operationTimeoutMs`) ?? 5000,
                token: optionalString(source["token"], `${path}.token`),
                tlsCaFile: optionalString(source.tlsCaFile, `${path}.tlsCaFile`),
                tlsCertFile: optionalString(source.tlsCertFile, `${path}.tlsCertFile`),
                tlsKeyFile: optionalString(source.tlsKeyFile, `${path}.tlsKeyFile`)
            });
        }
        case 10: {
            const servers = source["servers"];
            if (!Array.isArray(servers) || servers.some((s) => typeof s !== "string"))
                throw new Error(`${path}.servers must be an array`);
            return requireRedisStreamsDataConnectorConfig({
                ...common,
                type: 10,
                servers: [...servers],
                redisDatabase: optionalInteger(source["redisDatabase"], `${path}.redisDatabase`) ?? 0,
                connectTimeoutMs: optionalInteger(source["connectTimeoutMs"], `${path}.connectTimeoutMs`) ?? 2000,
                operationTimeoutMs: optionalInteger(source["operationTimeoutMs"], `${path}.operationTimeoutMs`) ?? 5000,
                username: optionalString(source.username, `${path}.username`),
                password: optionalString(source.password, `${path}.password`),
                token: optionalString(source["token"], `${path}.token`),
                credentialsFile: optionalString(source["credentialsFile"], `${path}.credentialsFile`),
                tlsEnabled: optionalBoolean(source.tlsEnabled, `${path}.tlsEnabled`),
                tlsServerName: optionalString(source.tlsServerName, `${path}.tlsServerName`),
                tlsCaFile: optionalString(source.tlsCaFile, `${path}.tlsCaFile`),
                tlsCertFile: optionalString(source.tlsCertFile, `${path}.tlsCertFile`),
                tlsKeyFile: optionalString(source.tlsKeyFile, `${path}.tlsKeyFile`)
            });
        }
        case 7:
        case 8:
        case 9: {
            const servers = source["servers"];
            if (!Array.isArray(servers) ||
                servers.length === 0 ||
                servers.some((s) => typeof s !== "string" || !s))
                throw new Error(`${path}.servers must be a nonempty URL array`);
            const connectTimeoutMs = optionalInteger(source["connectTimeoutMs"], `${path}.connectTimeoutMs`) ?? 2000;
            const operationTimeoutMs = optionalInteger(source["operationTimeoutMs"], `${path}.operationTimeoutMs`) ?? 5000;
            if (connectTimeoutMs <= 0 || operationTimeoutMs <= 0)
                throw new Error(`${path} native broker timeouts must be positive milliseconds`);
            return (type === 9 ? requireRabbitMQDataConnectorConfig : requireNATSDataConnectorConfig)({
                ...common,
                type,
                servers: type === 9 ? [...servers] : servers,
                ...(type === 9
                    ? { virtualHost: optionalString(source["virtualHost"], `${path}.virtualHost`) }
                    : {}),
                connectTimeoutMs,
                operationTimeoutMs,
                username: optionalString(source.username, `${path}.username`),
                password: optionalString(source.password, `${path}.password`),
                token: optionalString(source["token"], `${path}.token`),
                credentialsFile: optionalString(source["credentialsFile"], `${path}.credentialsFile`),
                tlsEnabled: optionalBoolean(source.tlsEnabled, `${path}.tlsEnabled`) ?? false,
                tlsServerName: optionalString(source.tlsServerName, `${path}.tlsServerName`),
                tlsCaFile: optionalString(source.tlsCaFile, `${path}.tlsCaFile`),
                tlsCertFile: optionalString(source.tlsCertFile, `${path}.tlsCertFile`),
                tlsKeyFile: optionalString(source.tlsKeyFile, `${path}.tlsKeyFile`)
            });
        }
        default:
            throw new Error(`${path}.type has an unknown data connector type`);
    }
}
const endpointKeys = new Set([
    "queryOperation", "fetchSize", "sqlFile", "bucket",
    "subscriptionName",
    "subscriptionType",
    "createSubscription",
    "subscriptionInitialPosition",
    "receiverQueueSize",
    "negativeAckRedeliveryDelayMs",
    "streamKey",
    "consumerName",
    "createConsumerGroup",
    "groupStartId",
    "readCount",
    "readBlockMs",
    "claimIdleMs",
    "claimIntervalMs",
    "queueName",
    "queueType",
    "exchangeName",
    "exchangeType",
    "routingKey",
    "createQueue",
    "createExchange",
    "createBinding",
    "consumerCredits",
    "publisherMaxInFlight",
    ...identityKeys,
    "subject",
    "queueGroup",
    "streamName",
    "durableName",
    "createStream",
    "createConsumer",
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
    "temporalExecutionType",
    "maxConcurrentActivities",
    "maxConcurrentWorkflowTasks",
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
function parseEndpoint(source, path, connectorType) {
    const common = {
        ...identity(source, path),
        idDataConnector: integer(source.idDataConnector, `${path}.idDataConnector`),
        tracingEnabled: optionalBoolean(source["tracingEnabled"], `${path}.tracingEnabled`) ?? false,
        properties: properties(source, endpointKeys)
    };
    const functions = functionFields(source, path);
    if (connectorType === 13 || connectorType === 18) {
        return requireQueryEndpointConfig({
            ...common, ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? true,
            queryOperation: stringValue(source["queryOperation"], `${path}.queryOperation`),
            fetchSize: optionalInteger(source["fetchSize"], `${path}.fetchSize`) ?? 256,
            sqlFile: optionalString(source["sqlFile"], `${path}.sqlFile`),
            bucket: optionalString(source["bucket"], `${path}.bucket`)
        }, connectorType);
    }
    if (connectorType === 11) {
        for (const k of [
            "consumerGroup",
            "replicationFactor",
            "subject",
            "streamKey",
            "queueName",
            "httpMethodType",
            "grpcMethodType",
            "schedule",
            "taskQueue"
        ])
            if (source[k] !== undefined)
                throw new Error(`${path}.${k} is not a Pulsar endpoint field`);
        return requirePulsarEndpointConfig({
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            topic: optionalString(source.topic, `${path}.topic`) ?? "",
            createTopic: optionalBoolean(source.createTopic, `${path}.createTopic`),
            createSubscription: optionalBoolean(source["createSubscription"], `${path}.createSubscription`),
            partitions: optionalInteger(source.partitions, `${path}.partitions`),
            receiverQueueSize: optionalInteger(source["receiverQueueSize"], `${path}.receiverQueueSize`),
            negativeAckRedeliveryDelayMs: optionalInteger(source["negativeAckRedeliveryDelayMs"], `${path}.negativeAckRedeliveryDelayMs`),
            publisherMaxInFlight: optionalInteger(source["publisherMaxInFlight"], `${path}.publisherMaxInFlight`),
            subscriptionName: optionalString(source["subscriptionName"], `${path}.subscriptionName`),
            subscriptionType: optionalEnum(source["subscriptionType"], `${path}.subscriptionType`, [
                "Shared",
                "Exclusive",
                "Failover"
            ]),
            subscriptionInitialPosition: optionalEnum(source["subscriptionInitialPosition"], `${path}.subscriptionInitialPosition`, ["Earliest", "Latest"])
        });
    }
    for (const k of [
        "subscriptionName",
        "subscriptionType",
        "createSubscription",
        "subscriptionInitialPosition",
        "receiverQueueSize",
        "negativeAckRedeliveryDelayMs"
    ])
        if (source[k] !== undefined)
            throw new Error(`${path}.${k} requires Pulsar`);
    if (source["streamKey"] !== undefined ||
        source["consumerName"] !== undefined ||
        source["createConsumerGroup"] !== undefined ||
        source["groupStartId"] !== undefined ||
        source["readCount"] !== undefined ||
        source["readBlockMs"] !== undefined ||
        source["claimIdleMs"] !== undefined ||
        source["claimIntervalMs"] !== undefined) {
        if ([
            "subject",
            "queueGroup",
            "durableName",
            "streamName",
            "createConsumer",
            "topic",
            "queueName",
            "exchangeName",
            "createQueue",
            "httpMethodType",
            "grpcMethodType",
            "taskQueue",
            "schedule"
        ].some((k) => source[k] !== undefined))
            throw new Error(`${path} mixes RedisStreams and other endpoint fields`);
        return requireRedisStreamsEndpointConfig({
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            streamKey: optionalString(source["streamKey"], `${path}.streamKey`) ?? "",
            consumerName: optionalString(source["consumerName"], `${path}.consumerName`),
            createConsumerGroup: optionalBoolean(source["createConsumerGroup"], `${path}.createConsumerGroup`),
            groupStartId: optionalString(source["groupStartId"], `${path}.groupStartId`),
            readCount: optionalInteger(source["readCount"], `${path}.readCount`),
            readBlockMs: optionalInteger(source["readBlockMs"], `${path}.readBlockMs`),
            claimIdleMs: optionalInteger(source["claimIdleMs"], `${path}.claimIdleMs`),
            claimIntervalMs: optionalInteger(source["claimIntervalMs"], `${path}.claimIntervalMs`),
            consumerGroup: optionalString(source.consumerGroup, `${path}.consumerGroup`),
            createStream: optionalBoolean(source["createStream"], `${path}.createStream`),
            publisherMaxInFlight: optionalInteger(source["publisherMaxInFlight"], `${path}.publisherMaxInFlight`)
        });
    }
    if (source["queueName"] !== undefined ||
        source["queueType"] !== undefined ||
        source["exchangeName"] !== undefined ||
        source["exchangeType"] !== undefined ||
        source["routingKey"] !== undefined ||
        source["createQueue"] !== undefined ||
        source["createExchange"] !== undefined ||
        source["createBinding"] !== undefined ||
        source["consumerCredits"] !== undefined ||
        source["publisherMaxInFlight"] !== undefined) {
        if ([
            "subject",
            "queueGroup",
            "streamName",
            "durableName",
            "createStream",
            "createConsumer",
            "topic",
            "httpMethodType",
            "grpcMethodType",
            "taskQueue",
            "schedule"
        ].some((k) => source[k] !== undefined))
            throw new Error(`${path} mixes RabbitMQ and other endpoints`);
        return requireRabbitMQEndpointConfig({
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            queueName: optionalString(source["queueName"], `${path}.queueName`),
            queueType: optionalString(source["queueType"], `${path}.queueType`),
            exchangeName: optionalString(source["exchangeName"], `${path}.exchangeName`),
            exchangeType: optionalString(source["exchangeType"], `${path}.exchangeType`),
            routingKey: optionalString(source["routingKey"], `${path}.routingKey`),
            createQueue: optionalBoolean(source["createQueue"], `${path}.createQueue`) ?? false,
            createExchange: optionalBoolean(source["createExchange"], `${path}.createExchange`) ?? false,
            createBinding: optionalBoolean(source["createBinding"], `${path}.createBinding`) ?? false,
            consumerCredits: optionalInteger(source["consumerCredits"], `${path}.consumerCredits`),
            publisherMaxInFlight: optionalInteger(source["publisherMaxInFlight"], `${path}.publisherMaxInFlight`)
        });
    }
    if (["subject", "queueGroup", "streamName", "durableName", "createStream", "createConsumer"].some((key) => source[key] !== undefined)) {
        if (["topic", "consumerGroup", "httpMethodType", "grpcMethodType", "taskQueue", "schedule"].some((key) => source[key] !== undefined))
            throw new Error(`${path} mixes NATS and other endpoint fields`);
        return {
            ...common,
            ...functions,
            enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false,
            subject: optionalString(source["subject"], `${path}.subject`) ?? "",
            queueGroup: optionalString(source["queueGroup"], `${path}.queueGroup`),
            streamName: optionalString(source["streamName"], `${path}.streamName`),
            durableName: optionalString(source["durableName"], `${path}.durableName`),
            createStream: optionalBoolean(source["createStream"], `${path}.createStream`) ?? false,
            createConsumer: optionalBoolean(source["createConsumer"], `${path}.createConsumer`) ?? false
        };
    }
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
            temporalExecutionType: requiredEnum(source.temporalExecutionType, `${path}.temporalExecutionType`, ["Activity", "Workflow"]),
            maxConcurrentActivities: optionalInteger(source.maxConcurrentActivities, `${path}.maxConcurrentActivities`) ?? 0,
            maxConcurrentWorkflowTasks: optionalInteger(source.maxConcurrentWorkflowTasks, `${path}.maxConcurrentWorkflowTasks`) ??
                0,
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
    return {
        ...common,
        ...functions,
        enabled: optionalBoolean(source.enabled, `${path}.enabled`) ?? false
    };
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
    const dataConnectors = namedSection(root, "dataConnectors", parseConnector);
    const connectorTypes = new Map(dataConnectors.map((c) => [c.id, c.type]));
    return {
        services: namedSection(root, "services", parseService),
        streams: namedSection(root, "streams", parseStream),
        dataConnectors,
        endpoints: namedSection(root, "endpoints", (source, path) => parseEndpoint(source, path, connectorTypes.get(integer(source.idDataConnector, `${path}.idDataConnector`)))),
        pools: namedSection(root, "pools", parsePool),
        links: namedSection(root, "links", parseLink),
        modules: namedSection(root, "modules", parseModule),
        types: namedSection(root, "types", parseType),
        properties: properties(root, rootKeys)
    };
}
export const canonicalConfigSchema = { parse: parseCanonicalConfig };
//# sourceMappingURL=parser.js.map