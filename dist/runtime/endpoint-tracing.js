import { stringAttribute } from "./environment/tracing/tracing.js";
/** Construct immutable endpoint definition attributes once, outside request processing. */
export function makeEndpointTraceAttributes(stream, endpointName) {
    const config = stream.runtimeEnvironment().runtimeConfig().streamById(stream.id);
    return Object.freeze([
        Object.freeze(stringAttribute("stream", stream.name)),
        Object.freeze(stringAttribute("endpoint", endpointName)),
        Object.freeze(stringAttribute("pipeline", config?.pipeline ?? "")),
        Object.freeze(stringAttribute("component", config?.component ?? ""))
    ]);
}
//# sourceMappingURL=endpoint-tracing.js.map