function address(value, schemes) {
    try {
        const u = new URL(value);
        if (!schemes.includes(u.protocol) ||
            !u.hostname ||
            (u.pathname !== "" && u.pathname !== "/") ||
            /[@?#\s\p{Cc}]/u.test(value) ||
            value.endsWith(":"))
            throw new Error();
        return u;
    }
    catch {
        throw new Error("Pulsar requires a service/admin URL without credentials, path, query or fragment");
    }
}
function bound(name, value, max) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1 || value > max))
        throw new Error(`Pulsar ${name} requires 1..${String(max)}`);
}
export function requirePulsarDataConnectorConfig(value) {
    if (value?.type !== 11 || !("servers" in value) || !("adminUrl" in value))
        throw new Error("invalid Pulsar connector config");
    const c = value;
    if (c.implementation !== "pulsar-client-node")
        throw new Error("Pulsar requires official pulsar-client-node");
    if (!Array.isArray(c.servers) || c.servers.length !== 1 || typeof c.servers[0] !== "string")
        throw new Error("Pulsar requires one service URL");
    const service = address(c.servers[0], ["pulsar:", "pulsar+ssl:"]);
    const admin = address(c.adminUrl, ["http:", "https:"]);
    if (service.protocol === "pulsar+ssl:" && admin.protocol !== "https:")
        throw new Error("Pulsar TLS requires HTTPS administration");
    if ((c.tlsCaFile || c.tlsCertFile || c.tlsKeyFile) &&
        (service.protocol !== "pulsar+ssl:" || admin.protocol !== "https:"))
        throw new Error("Pulsar TLS files require secure URLs");
    if (!!c.tlsCertFile !== !!c.tlsKeyFile)
        throw new Error("Pulsar TLS identity requires certificate and key");
    if (c.adminToken !== undefined &&
        (typeof c.adminToken !== "string" || /[\r\n]/u.test(c.adminToken)))
        throw new Error("Pulsar adminToken must be a string without header control characters");
    if ((c.token || c.adminToken) && c.tlsCertFile)
        throw new Error("select Pulsar token or TLS identity");
    for (const key of ["username", "password", "credentialsFile", "tlsEnabled", "tlsServerName"])
        if (key in c || key in c.properties)
            throw new Error(`unsupported Pulsar option ${key}`);
    bound("connectTimeoutMs", c.connectTimeoutMs, 2147483647);
    bound("operationTimeoutMs", c.operationTimeoutMs, 2147483647);
    return c;
}
export function requirePulsarEndpointConfig(value) {
    if (!value || !("topic" in value) || typeof value.topic !== "string")
        throw new Error("invalid Pulsar endpoint config");
    return value;
}
export function validatePulsarEndpoint(c, sink) {
    if (typeof c.enabled !== "boolean" ||
        !/^persistent:\/\/[^/]+\/[^/]+\/[^/]+$/u.test(c.topic) ||
        /[%?#\\\s\p{Cc}]/u.test(c.topic) ||
        c.topic
            .split("/")
            .slice(2)
            .some((p) => p === "." || p === ".."))
        throw new Error("Pulsar requires a fully qualified persistent topic and boolean enabled");
    for (const flag of [c.createTopic, c.createSubscription])
        if (flag !== undefined && typeof flag !== "boolean")
            throw new Error("Pulsar creation flags must be boolean");
    if (c.partitions !== undefined &&
        (!Number.isSafeInteger(c.partitions) ||
            c.partitions < 1 ||
            c.partitions > 1000 ||
            !c.createTopic))
        throw new Error("Pulsar partitions require explicit topic creation and range 1..1000");
    bound("receiverQueueSize", c.receiverQueueSize, 4096);
    bound("negativeAckRedeliveryDelayMs", c.negativeAckRedeliveryDelayMs, 2147483647);
    bound("publisherMaxInFlight", c.publisherMaxInFlight, 4096);
    if (sink) {
        if ([
            c.subscriptionName,
            c.subscriptionType,
            c.createSubscription,
            c.subscriptionInitialPosition,
            c.receiverQueueSize,
            c.negativeAckRedeliveryDelayMs
        ].some((v) => v !== undefined))
            throw new Error("Pulsar sink cannot set subscription options");
    }
    else {
        if (!c.subscriptionName || /[/%?#\\\p{Cc}]/u.test(c.subscriptionName))
            throw new Error("Pulsar source requires subscriptionName");
        if (c.subscriptionType !== undefined &&
            !["Shared", "Exclusive", "Failover"].includes(c.subscriptionType))
            throw new Error("unsupported Pulsar subscriptionType");
        if (c.subscriptionInitialPosition !== undefined &&
            (!c.createSubscription || !["Earliest", "Latest"].includes(c.subscriptionInitialPosition)))
            throw new Error("Pulsar initial position requires explicit subscription creation");
        if (c.publisherMaxInFlight !== undefined)
            throw new Error("Pulsar source cannot configure publisher admission");
    }
}
//# sourceMappingURL=pulsar.js.map