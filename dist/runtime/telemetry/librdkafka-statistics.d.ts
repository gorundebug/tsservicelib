import type { Metrics } from "../environment/index.js";
type KafkaClientRole = "consumer" | "producer";
export interface LibrdkafkaStatisticsOptions {
    readonly "statistics.interval.ms": number;
    readonly stats_cb: (event: unknown) => void;
}
/** Enables librdkafka's documented periodic statistics callback off the message hot path. */
export declare function librdkafkaStatisticsOptions(metrics: Metrics, role: KafkaClientRole): LibrdkafkaStatisticsOptions | undefined;
export {};
//# sourceMappingURL=librdkafka-statistics.d.ts.map