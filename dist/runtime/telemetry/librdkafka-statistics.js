/** Enables librdkafka's documented periodic statistics callback off the message hot path. */
export function librdkafkaStatisticsOptions(metrics, role) {
    if (!metrics.enabled())
        return undefined;
    const observer = new LibrdkafkaStatisticsObserver(metrics, role);
    return {
        "statistics.interval.ms": 1000,
        stats_cb: (event) => {
            observer.update(event);
        }
    };
}
class LibrdkafkaStatisticsObserver {
    #brokers;
    #brokersUp;
    #replyQueueMessages;
    #messagesQueued;
    #messageBytesQueued;
    #requestsSent;
    #responsesReceived;
    #bytesSent;
    #bytesReceived;
    #messagesSent;
    #messagesReceived;
    #consumerLag;
    constructor(metrics, role) {
        const scope = metrics.scope("kafka_client", { role });
        this.#brokers = scope.gauge("brokers", "Brokers known to this librdkafka client");
        this.#brokersUp = scope.gauge("brokers_up", "Brokers currently connected");
        this.#replyQueueMessages = scope.gauge("reply_queue_messages", "Operations waiting in the librdkafka reply queue");
        this.#messagesQueued = scope.gauge("messages_queued", "Messages currently queued in librdkafka");
        this.#messageBytesQueued = scope.gauge("message_bytes_queued", "Message bytes currently queued in librdkafka");
        this.#requestsSent = scope.gauge("requests_sent", "Requests sent since this librdkafka client was created");
        this.#responsesReceived = scope.gauge("responses_received", "Responses received since this librdkafka client was created");
        this.#bytesSent = scope.gauge("bytes_sent", "Bytes sent since this librdkafka client was created");
        this.#bytesReceived = scope.gauge("bytes_received", "Bytes received since this librdkafka client was created");
        this.#messagesSent = scope.gauge("messages_sent", "Messages sent since this librdkafka client was created");
        this.#messagesReceived = scope.gauge("messages_received", "Messages received since this librdkafka client was created");
        this.#consumerLag = scope.gauge("consumer_lag", "Sum of non-negative lag for assigned partitions");
    }
    update(event) {
        const statistics = parseStatistics(event);
        if (statistics === undefined)
            return;
        setInteger(this.#replyQueueMessages, statistics.replyq);
        setInteger(this.#messagesQueued, statistics.msg_cnt);
        setInteger(this.#messageBytesQueued, statistics.msg_size);
        setInteger(this.#requestsSent, statistics.tx);
        setInteger(this.#responsesReceived, statistics.rx);
        setInteger(this.#bytesSent, statistics.tx_bytes);
        setInteger(this.#bytesReceived, statistics.rx_bytes);
        setInteger(this.#messagesSent, statistics.txmsgs);
        setInteger(this.#messagesReceived, statistics.rxmsgs);
        const brokers = record(statistics.brokers);
        if (brokers !== undefined) {
            const values = Object.values(brokers);
            this.#brokers.set(values.length);
            this.#brokersUp.set(values.filter((broker) => record(broker)?.["state"] === "UP").length);
        }
        this.#consumerLag.set(totalConsumerLag(statistics.topics));
    }
}
function parseStatistics(event) {
    const wrapped = record(event);
    const value = wrapped?.["message"] ?? event;
    if (typeof value === "string") {
        try {
            return record(JSON.parse(value));
        }
        catch {
            return undefined;
        }
    }
    return record(value);
}
function totalConsumerLag(topicsValue) {
    const topics = record(topicsValue);
    if (topics === undefined)
        return 0;
    let total = 0;
    for (const topic of Object.values(topics)) {
        const partitions = record(record(topic)?.["partitions"]);
        if (partitions === undefined)
            continue;
        for (const partition of Object.values(partitions)) {
            const lag = record(partition)?.["consumer_lag"];
            if (isSafeInteger(lag) && lag >= 0)
                total += lag;
        }
    }
    return Number.isSafeInteger(total) ? total : 0;
}
function setInteger(gauge, value) {
    if (isSafeInteger(value))
        gauge.set(value);
}
function isSafeInteger(value) {
    return typeof value === "number" && Number.isSafeInteger(value);
}
function record(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value)
        ? value
        : undefined;
}
//# sourceMappingURL=librdkafka-statistics.js.map