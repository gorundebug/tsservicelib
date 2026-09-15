import type { Stream } from "./stream.js";
import { type Attribute } from "./environment/tracing/tracing.js";
/** Construct immutable endpoint definition attributes once, outside request processing. */
export declare function makeEndpointTraceAttributes(stream: Stream, endpointName: string): readonly Attribute[];
//# sourceMappingURL=endpoint-tracing.d.ts.map