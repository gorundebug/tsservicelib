import type { QueryStream } from "../operators/query.js";
/** Bind topology-only handles once. Never inspect SQL, keys or result values. */
export declare function endpointMetrics<T, R, E>(stream: QueryStream<T, R, E>): {
    requests: import("../index.js").Int64Counter;
    results: import("../index.js").Int64Counter;
    errors: import("../index.js").Int64Counter;
    cancellations: import("../index.js").Int64Counter;
    active: import("../index.js").Int64Gauge;
    duration: import("../index.js").Float64Histogram;
} | undefined;
//# sourceMappingURL=metrics.d.ts.map