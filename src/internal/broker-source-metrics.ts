import type { Context } from "../runtime/context.js";
import type { Metrics, Int64Counter } from "../runtime/environment/metrics/index.js";

/** Native observations; no settlement or business-completion inference. */
export class SourceMetrics {
  readonly #counters = new Map<string, Int64Counter>();
  public readonly enabled: boolean;
  public constructor(metrics: Metrics, protocol: string, connector: string, endpoint: string) {
    this.enabled = metrics.enabled();
    if (!this.enabled) return;
    const scope = metrics.scope("broker_endpoint", { protocol, connector, endpoint });
    const observations = ["received"];
    if (["jetstream", "pulsar", "rabbitmq"].includes(protocol)) observations.push("redelivered");
    else if (protocol === "redisstreams") observations.push("reclaimed");
    for (const observation of observations) this.#counters.set(observation, scope.counter(
      "source_observations_total",
      "Observed native source deliveries; not acknowledgements or business completion", { observation }
    ));
  }
  public observe(context: Context, observation: string): void {
    this.#counters.get(observation)?.inc(context);
  }
}
