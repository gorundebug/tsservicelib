import { DataConnectorType } from "./types.js";
export function isCustomDataConnectorConfig(value) {
    return value?.type === DataConnectorType.Custom;
}
export function requireCustomDataConnectorConfig(value) {
    if (!isCustomDataConnectorConfig(value)) {
        throw new Error("invalid Custom data connector config");
    }
    return value;
}
export function isCustomEndpointConfig(value) {
    return value !== undefined;
}
export function requireCustomEndpointConfig(value) {
    if (!isCustomEndpointConfig(value)) {
        throw new Error("invalid Custom endpoint config");
    }
    return value;
}
//# sourceMappingURL=custom.js.map