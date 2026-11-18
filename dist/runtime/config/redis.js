export function requireRedisStreamsDataConnectorConfig(value) {
    if (value?.type !== 10 || !("servers" in value))
        throw new Error("invalid RedisStreams connector config");
    const c = value;
    if (c.implementation !== "node-redis")
        throw new Error("RedisStreams requires official node-redis implementation");
    if (!Array.isArray(c.servers) || c.servers.length !== 1)
        throw new Error("RedisStreams supports one standalone primary only; Cluster and Sentinel are unsupported");
    const servers = c.servers;
    let secure = false;
    for (const server of servers) {
        try {
            const u = new URL(server);
            secure = u.protocol === "rediss:";
            if (!["redis:", "rediss:"].includes(u.protocol) ||
                !u.hostname ||
                server.endsWith(":") ||
                (!!u.port && (+u.port < 1 || +u.port > 65535)) ||
                u.pathname ||
                /[@?#\s\p{Cc}]/u.test(server))
                throw new Error();
        }
        catch {
            throw new Error("RedisStreams URL requires redis/rediss host and optional port, without credentials/database/path/query/fragment");
        }
    }
    if (c.username && !c.password)
        throw new Error("RedisStreams username requires password");
    if (c.token || c.credentialsFile)
        throw new Error("RedisStreams does not support token or credentialsFile");
    if (!!c.tlsCertFile !== !!c.tlsKeyFile)
        throw new Error("RedisStreams TLS keypair must be paired");
    if (!(secure || c.tlsEnabled) &&
        (c.tlsServerName || c.tlsCaFile || c.tlsCertFile || c.tlsKeyFile))
        throw new Error("RedisStreams TLS fields require TLS transport");
    for (const ms of [c.connectTimeoutMs, c.operationTimeoutMs])
        if (!Number.isSafeInteger(ms) || ms < 1 || ms > 2147483647)
            throw new Error("RedisStreams timeouts require 1..2147483647 milliseconds");
    if (!Number.isSafeInteger(c.redisDatabase) || c.redisDatabase < 0 || c.redisDatabase > 2147483647)
        throw new Error("RedisStreams database must be a nonnegative integer");
    return c;
}
export function requireRedisStreamsEndpointConfig(value) {
    if (!value || !("streamKey" in value) || typeof value.streamKey !== "string")
        throw new Error("invalid RedisStreams endpoint config");
    return value;
}
export function validateRedisStreamsEndpoint(c, sink) {
    if (!c.streamKey || typeof c.enabled !== "boolean")
        throw new Error("RedisStreams requires streamKey and boolean enabled");
    for (const v of [c.createConsumerGroup, c.createStream])
        if (v !== undefined && typeof v !== "boolean")
            throw new Error("RedisStreams creation flags must be booleans");
    if (sink) {
        if (c.consumerGroup !== undefined ||
            c.consumerName !== undefined ||
            c.createConsumerGroup !== undefined ||
            c.groupStartId !== undefined ||
            c.readCount !== undefined ||
            c.readBlockMs !== undefined ||
            c.claimIdleMs !== undefined ||
            c.claimIntervalMs !== undefined)
            throw new Error("RedisStreams sink cannot configure a source group/read/reclaim");
    }
    else {
        if (!c.consumerGroup)
            throw new Error("RedisStreams source requires consumerGroup");
        if (c.createStream !== undefined || c.publisherMaxInFlight !== undefined)
            throw new Error("RedisStreams source cannot configure sink creation/admission");
        if (c.groupStartId !== undefined && !c.createConsumerGroup)
            throw new Error("RedisStreams groupStartId requires createConsumerGroup");
    }
    for (const [name, v, max] of [
        ["readCount", c.readCount, 4096],
        ["readBlockMs", c.readBlockMs, 60000],
        ["claimIdleMs", c.claimIdleMs, 2147483647],
        ["claimIntervalMs", c.claimIntervalMs, 2147483647],
        ["publisherMaxInFlight", c.publisherMaxInFlight, 4096]
    ])
        if (v !== undefined && (!Number.isSafeInteger(v) || v < 1 || v > max))
            throw new Error(`RedisStreams ${name} requires 1..${String(max)}`);
    if (c.groupStartId !== undefined && !["0-0", "$"].includes(c.groupStartId))
        throw new Error("RedisStreams groupStartId requires 0-0 or $");
    if (c.consumerName !== undefined && !c.consumerName)
        throw new Error("RedisStreams consumerName cannot be empty");
}
//# sourceMappingURL=redis.js.map