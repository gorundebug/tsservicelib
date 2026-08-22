export function isHttpDataConnectorConfig(value) {
    return (value?.type === 1 &&
        "useDedicatedListener" in value &&
        typeof value.useDedicatedListener === "boolean" &&
        (!("host" in value) || value.host === undefined || typeof value.host === "string") &&
        (!("port" in value) ||
            value.port === undefined ||
            (typeof value.port === "number" &&
                Number.isSafeInteger(value.port) &&
                value.port >= 0 &&
                value.port <= 65_535)) &&
        (!("module" in value) || value.module === undefined || typeof value.module === "string"));
}
export function requireHttpDataConnectorConfig(value) {
    if (!isHttpDataConnectorConfig(value)) {
        throw new Error("invalid HTTP data connector config");
    }
    return value;
}
export function isHttpEndpointConfig(value) {
    return (value !== undefined &&
        "httpMethodType" in value &&
        (value.httpMethodType === "GET" || value.httpMethodType === "POST") &&
        "path" in value &&
        typeof value.path === "string");
}
export function requireHttpEndpointConfig(value) {
    if (!isHttpEndpointConfig(value)) {
        throw new Error("invalid HTTP endpoint config");
    }
    return value;
}
//# sourceMappingURL=http.js.map