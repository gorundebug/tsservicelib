/** Native observations; no settlement or business-completion inference. */
export class SourceMetrics {
    #counters = new Map();
    enabled;
    constructor(metrics, protocol, connector, endpoint) {
        this.enabled = metrics.enabled();
        if (!this.enabled)
            return;
        const scope = metrics.scope("broker_endpoint", { protocol, connector, endpoint });
        const observations = ["received"];
        if (["jetstream", "pulsar", "rabbitmq"].includes(protocol))
            observations.push("redelivered");
        else if (protocol === "redisstreams")
            observations.push("reclaimed");
        for (const observation of observations)
            this.#counters.set(observation, scope.counter("source_observations_total", "Observed native source deliveries; not acknowledgements or business completion", { observation }));
    }
    observe(context, observation) {
        this.#counters.get(observation)?.inc(context);
    }
}
//# sourceMappingURL=broker-source-metrics.js.map