import type { QueryStream } from "../operators/query.js";
import type { QueryEndpointConfig } from "../runtime/config/types.js";

/** Bind topology-only handles once. Never inspect SQL, keys or result values. */
export function endpointMetrics<T, R, E>(stream: QueryStream<T, R, E>) {
  const environment = stream.runtimeEnvironment();
  const metrics = environment.metrics();
  if (!metrics.enabled()) return undefined;
  const config = environment.runtimeConfig();
  const endpoint = config.endpointById(stream.endpointId()) as QueryEndpointConfig | undefined;
  if (endpoint?.queryOperation === undefined) throw new Error("Query metrics require a Query endpoint configuration");
  const connector = config.dataConnectorById(endpoint.idDataConnector);
  if (connector === undefined) throw new Error("Query endpoint has no connector");
  const scope = metrics.scope("dataquery_endpoint", {
    connector: connector.name, endpoint: endpoint.name, operation: endpoint.queryOperation
  });
  const failures = scope.counterVec("failures_total", "Failed Query endpoint requests");
  return {
    requests: scope.counter("requests_total", "Admitted Query endpoint requests"),
    results: scope.counter("results_total", "Query result collector emissions, including application completion"),
    errors: failures.with({ outcome: "error" }),
    cancellations: failures.with({ outcome: "cancelled" }),
    active: scope.gauge("active_requests", "Active Query endpoint requests through EndRequest"),
    duration: scope.histogram("request_duration_seconds", "Query request duration through EndRequest in seconds")
  };
}
