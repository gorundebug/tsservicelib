function hasString(value, key) {
    return key in value && typeof Reflect.get(value, key) === "string";
}
function hasNumber(value, key) {
    const field = Reflect.get(value, key);
    return key in value && typeof field === "number" && Number.isFinite(field);
}
function hasBoolean(value, key) {
    return key in value && typeof Reflect.get(value, key) === "boolean";
}
function requireStream(value, name, guard) {
    if (value === undefined || !guard(value)) {
        throw new Error(`invalid ${name} stream config`);
    }
    return value;
}
export function isInputStreamConfig(value) {
    return value.type === "Input" && hasString(value, "valueType") && hasNumber(value, "idEndpoint");
}
export function requireInputStreamConfig(value) {
    return requireStream(value, "Input", isInputStreamConfig);
}
export function isMapStreamConfig(value) {
    return value.type === "Map" && hasString(value, "valueType");
}
export function requireMapStreamConfig(value) {
    return requireStream(value, "Map", isMapStreamConfig);
}
export function isFilterStreamConfig(value) {
    return value.type === "Filter";
}
export function requireFilterStreamConfig(value) {
    return requireStream(value, "Filter", isFilterStreamConfig);
}
export function isJoinStreamConfig(value) {
    return (value.type === "Join" &&
        hasString(value, "valueType") &&
        hasNumber(value, "joinType") &&
        hasNumber(value, "joinStorage") &&
        hasNumber(value, "ttl") &&
        hasBoolean(value, "renewTTL"));
}
export function requireJoinStreamConfig(value) {
    return requireStream(value, "Join", isJoinStreamConfig);
}
export function isMultiJoinStreamConfig(value) {
    return (value.type === "MultiJoin" &&
        hasString(value, "valueType") &&
        hasNumber(value, "joinStorage") &&
        hasNumber(value, "ttl") &&
        hasBoolean(value, "renewTTL"));
}
export function requireMultiJoinStreamConfig(value) {
    return requireStream(value, "MultiJoin", isMultiJoinStreamConfig);
}
export function isProcessStreamConfig(value) {
    return value.type === "Process";
}
export function requireProcessStreamConfig(value) {
    return requireStream(value, "Process", isProcessStreamConfig);
}
export function isFlatMapStreamConfig(value) {
    return value.type === "FlatMap" && hasString(value, "valueType");
}
export function requireFlatMapStreamConfig(value) {
    return requireStream(value, "FlatMap", isFlatMapStreamConfig);
}
export function isFlatMapIterableStreamConfig(value) {
    return value.type === "FlatMapIterable" && hasString(value, "valueType");
}
export function requireFlatMapIterableStreamConfig(value) {
    return requireStream(value, "FlatMapIterable", isFlatMapIterableStreamConfig);
}
export function isKeyByStreamConfig(value) {
    return value.type === "KeyBy" && hasString(value, "keyType") && hasString(value, "valueType");
}
export function requireKeyByStreamConfig(value) {
    return requireStream(value, "KeyBy", isKeyByStreamConfig);
}
export function isMergeStreamConfig(value) {
    return value.type === "Merge";
}
export function requireMergeStreamConfig(value) {
    return requireStream(value, "Merge", isMergeStreamConfig);
}
export function isSplitStreamConfig(value) {
    return value.type === "Split";
}
export function requireSplitStreamConfig(value) {
    return requireStream(value, "Split", isSplitStreamConfig);
}
export function isCaseStreamConfig(value) {
    return value.type === "Case";
}
export function requireCaseStreamConfig(value) {
    return requireStream(value, "Case", isCaseStreamConfig);
}
export function isSinkStreamConfig(value) {
    return value.type === "Sink" && hasNumber(value, "idEndpoint");
}
export function requireSinkStreamConfig(value) {
    return requireStream(value, "Sink", isSinkStreamConfig);
}
export function isCycleLinkStreamConfig(value) {
    return value.type === "CycleLink";
}
export function requireCycleLinkStreamConfig(value) {
    return requireStream(value, "CycleLink", isCycleLinkStreamConfig);
}
export function isDelayStreamConfig(value) {
    return value.type === "Delay" && hasNumber(value, "duration");
}
export function requireDelayStreamConfig(value) {
    return requireStream(value, "Delay", isDelayStreamConfig);
}
export function isWhenStreamConfig(value) {
    return value.type === "When" && hasString(value, "valueType");
}
export function requireWhenStreamConfig(value) {
    return requireStream(value, "When", isWhenStreamConfig);
}
//# sourceMappingURL=streams.js.map