import type { Context } from "../runtime/context.js";
import type { Metrics, Int64Counter } from "../runtime/environment/metrics/index.js";

/** Fixed transport outcomes; never derived from payloads, IDs or error strings. */
export class PublicationMetrics {
  readonly #counters = new Map<string, Int64Counter>();
  public constructor(metrics: Metrics, protocol: string, connector: string, endpoint: string) {
    if (!metrics.enabled()) return;
    const scope = metrics.scope("broker_endpoint", { protocol, connector, endpoint });
    const outcomes =
      protocol === "nats" || protocol === "jetstream"
        ? ["client-accepted", "flushed", "acknowledged", "error"]
        : ["not-sent", "accepted", "rejected", "unknown", "unroutable", "modified"];
    for (const outcome of outcomes)
      this.#counters.set(
        outcome,
        scope.counter(
          "publication_outcomes_total",
          "Observed transport publication outcomes; not downstream business completion",
          { outcome }
        )
      );
  }
  public observe(context: Context, outcome: string): void {
    this.#counters.get(outcome === "NotSent" ? "not-sent" : outcome.toLowerCase())?.inc(context);
  }
}
