function invalidWhitespace(value) {
    if (/\s/u.test(value))
        return true;
    for (let i = 0; i < value.length; i++)
        if (value.charCodeAt(i) < 32)
            return true;
    return false;
}
export function requireNATSDataConnectorConfig(value) {
    if (value === undefined ||
        (value.type !== 7 && value.type !== 8) ||
        !("servers" in value) ||
        !Array.isArray(value.servers))
        throw new Error("invalid NATS/JetStream connector config");
    const config = value;
    if (!config.servers.length)
        throw new Error("NATS servers are required");
    for (const server of config.servers) {
        let url;
        try {
            url = new URL(server);
        }
        catch {
            throw new Error("invalid NATS server URL");
        }
        if (!["nats:", "tls:"].includes(url.protocol) ||
            !url.hostname ||
            url.username ||
            url.password ||
            url.pathname ||
            server.endsWith(":") ||
            server.includes("?") ||
            server.includes("#") ||
            url.search ||
            url.hash ||
            invalidWhitespace(server) ||
            (url.port !== "" && (Number(url.port) < 1 || Number(url.port) > 65535)))
            throw new Error("NATS Node TCP transport requires nats:// or tls:// URLs without credentials");
        if (config.tlsServerName && config.tlsServerName !== url.hostname)
            throw new Error("NATS.js Node transport cannot override TLS server name; use that hostname in servers");
    }
    if ((config.tlsServerName ||
        config.tlsCaFile ||
        config.tlsCertFile ||
        config.tlsKeyFile ||
        config.servers.some((s) => new URL(s).protocol === "tls:")) &&
        config.tlsEnabled !== true)
        throw new Error("NATS TLS fields and tls:// URLs require tlsEnabled=true");
    for (const ms of [config.connectTimeoutMs, config.operationTimeoutMs])
        if (!Number.isSafeInteger(ms) || ms <= 0 || ms > 9223372036854)
            throw new Error("NATS timeouts must be positive integer milliseconds");
    if ([!!config.token, !!config.credentialsFile, !!config.username || !!config.password].filter(Boolean).length > 1)
        throw new Error("NATS authentication modes are mutually exclusive");
    if (!!config.username !== !!config.password)
        throw new Error("NATS username/password must be paired");
    if (!!config.tlsCertFile !== !!config.tlsKeyFile)
        throw new Error("NATS TLS certificate/key must be paired");
    return config;
}
export function requireNATSEndpointConfig(value) {
    if (value === undefined ||
        !("subject" in value) ||
        typeof value.subject !== "string" ||
        !("enabled" in value) ||
        typeof value.enabled !== "boolean")
        throw new Error("invalid NATS endpoint config");
    return value;
}
export function validateNATSEndpoint(config, kind, sink) {
    for (const value of [config.createStream, config.createConsumer])
        if (typeof value !== "boolean")
            throw new Error("NATS resource creation flags must be booleans");
    for (const name of [config.streamName, config.durableName])
        if (name && (/[.*>/\\]/u.test(name) || invalidWhitespace(name)))
            throw new Error("invalid JetStream resource name");
    if (config.queueGroup &&
        (/[*>]/u.test(config.queueGroup) || invalidWhitespace(config.queueGroup)))
        throw new Error("invalid NATS queue group");
    const tokens = config.subject.split(".");
    if (!config.subject ||
        invalidWhitespace(config.subject) ||
        tokens.some((token, i) => !token ||
            (/[*>]/u.test(token) &&
                (sink || (token !== "*" && token !== ">") || (token === ">" && i !== tokens.length - 1)))))
        throw new Error("invalid NATS subject for endpoint direction");
    if (kind === 7 &&
        (config.streamName || config.durableName || config.createStream || config.createConsumer))
        throw new Error("Core NATS cannot configure JetStream resources");
    if (kind === 8 && (!config.streamName || config.queueGroup))
        throw new Error("JetStream requires streamName and forbids queueGroup");
    if (kind === 8 && !sink && !config.durableName)
        throw new Error("JetStream source requires durableName");
    if (sink && (config.queueGroup || config.durableName || config.createConsumer))
        throw new Error("NATS sink cannot configure a source consumer");
}
//# sourceMappingURL=nats.js.map