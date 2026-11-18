export function requireRabbitMQDataConnectorConfig(value) {
    if (value?.type !== 9 || !("servers" in value))
        throw new Error("invalid RabbitMQ connector config");
    const c = value;
    if (c.implementation !== "rabbitmq-amqp-js-client")
        throw new Error("RabbitMQ requires rabbitmq-amqp-js-client");
    if (!Array.isArray(c.servers) || !c.servers.length)
        throw new Error("RabbitMQ servers required");
    const servers = c.servers;
    for (const server of servers) {
        try {
            const u = new URL(server);
            if (!["amqp:", "amqps:"].includes(u.protocol) ||
                !u.hostname ||
                u.username ||
                u.password ||
                server.includes("@") ||
                u.pathname ||
                /[?#\s\p{Cc}]/u.test(server) ||
                server.endsWith(":") ||
                (u.port && (+u.port < 1 || +u.port > 65535)))
                throw new Error();
            if (c.tlsServerName && c.tlsServerName !== u.hostname)
                throw new Error();
        }
        catch {
            throw new Error("RabbitMQ requires native AMQP URLs without credentials/path/query/fragment and TLS name matching hostname");
        }
    }
    const tls = servers.some((s) => new URL(s).protocol === "amqps:");
    if (tls && servers.some((s) => new URL(s).protocol === "amqp:") && !c.tlsEnabled)
        throw new Error("mixed RabbitMQ URLs require tlsEnabled");
    if (!(tls || c.tlsEnabled) && (c.tlsCaFile || c.tlsCertFile || c.tlsKeyFile || c.tlsServerName))
        throw new Error("RabbitMQ TLS settings require TLS transport");
    if (!!c.username !== !!c.password || c.token || c.credentialsFile)
        throw new Error("RabbitMQ requires paired username/password; token and credentialsFile unsupported");
    if (!!c.tlsCertFile !== !!c.tlsKeyFile)
        throw new Error("RabbitMQ TLS keypair must be paired");
    if (c.virtualHost && c.virtualHost !== "/")
        throw new Error("RabbitMQ JS 1.0.0 public API supports only the default virtual host");
    for (const ms of [c.connectTimeoutMs, c.operationTimeoutMs])
        if (!Number.isSafeInteger(ms) || ms < 1 || ms > 2147483647)
            throw new Error("RabbitMQ JS timeouts require 1..2147483647 ms");
    return c;
}
export function requireRabbitMQEndpointConfig(value) {
    if (!value || !("createQueue" in value) || !("enabled" in value))
        throw new Error("invalid RabbitMQ endpoint config");
    return value;
}
export function validateRabbitMQEndpoint(c, sink) {
    for (const v of [c.enabled, c.createQueue, c.createExchange, c.createBinding])
        if (typeof v !== "boolean")
            throw new Error("RabbitMQ flags must be booleans");
    for (const v of [c.consumerCredits, c.publisherMaxInFlight])
        if (v !== undefined && (!Number.isInteger(v) || v < 1 || v > 4096))
            throw new Error("RabbitMQ limits require 1..4096");
    if (sink ? c.consumerCredits !== undefined : c.publisherMaxInFlight !== undefined)
        throw new Error("RabbitMQ limit belongs to opposite endpoint direction");
    if (!c.queueName && (!sink || !c.exchangeName))
        throw new Error("RabbitMQ source requires queueName; sink requires queueName or exchangeName");
    if (c.queueType && !["quorum", "classic"].includes(c.queueType))
        throw new Error("invalid RabbitMQ queueType");
    if (c.exchangeType &&
        (!c.createExchange || !["direct", "topic", "fanout"].includes(c.exchangeType)))
        throw new Error("RabbitMQ exchangeType requires createExchange and direct/topic/fanout");
    if (((c.createQueue || c.queueType) && !c.queueName) ||
        (c.createExchange && !c.exchangeName) ||
        (c.createBinding && (!c.queueName || !c.exchangeName)) ||
        (c.routingKey && !c.exchangeName))
        throw new Error("RabbitMQ provisioning flags require named resources");
    for (const v of [c.queueName, c.exchangeName, c.routingKey])
        if (v && /[\p{Cc}/?#%]/u.test(v))
            throw new Error("RabbitMQ JS resource names cannot contain URI delimiters or controls");
}
//# sourceMappingURL=rabbitmq.js.map