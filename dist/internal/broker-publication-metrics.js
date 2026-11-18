/** Fixed transport outcomes; never derived from payloads, IDs or error strings. */
export class PublicationMetrics {
    #counters = new Map();
    constructor(metrics, protocol, connector, endpoint) {
        if (!metrics.enabled())
            return;
        const scope = metrics.scope("broker_endpoint", { protocol, connector, endpoint });
        const outcomes = protocol === "nats" || protocol === "jetstream"
            ? ["client-accepted", "flushed", "acknowledged", "error"]
            : ["not-sent", "accepted", "rejected", "unknown", "unroutable", "modified"];
        for (const outcome of outcomes)
            this.#counters.set(outcome, scope.counter("publication_outcomes_total", "Observed transport publication outcomes; not downstream business completion", { outcome }));
    }
    observe(context, outcome) {
        this.#counters.get(outcome === "NotSent" ? "not-sent" : outcome.toLowerCase())?.inc(context);
    }
}
//# sourceMappingURL=broker-publication-metrics.js.map