export const SpanStatusCode = {
    Unset: "unset",
    Ok: "ok",
    Error: "error"
};
export function spanError(span, error) {
    if (span === undefined) {
        return;
    }
    span.recordError(error);
    span.setStatus(SpanStatusCode.Error, error.message);
}
export function stringAttribute(key, value) {
    return { key, type: "string", value };
}
export function int64Attribute(key, value) {
    return { key, type: "int64", value };
}
export function float64Attribute(key, value) {
    return { key, type: "float64", value };
}
export function boolAttribute(key, value) {
    return { key, type: "bool", value };
}
//# sourceMappingURL=tracing.js.map