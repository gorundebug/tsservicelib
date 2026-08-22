import assert from "node:assert/strict";
import { test } from "node:test";

import { PrometheusMetricsEngine } from "@gorundebug/tsservicelib/runtime";
import { librdkafkaStatisticsOptions } from "@gorundebug/tsservicelib/runtime/telemetry";

await test("documented librdkafka snapshots are exported without hot-path instrumentation", async () => {
  const metrics = new PrometheusMetricsEngine();
  const options = librdkafkaStatisticsOptions(metrics.metrics(), "consumer");
  if (options === undefined) throw new Error("Prometheus metrics unexpectedly disabled");
  assert.equal(options["statistics.interval.ms"], 1000);

  options.stats_cb({
    message: JSON.stringify({
      replyq: 2,
      msg_cnt: 3,
      msg_size: 128,
      tx: 11,
      rx: 12,
      tx_bytes: 1024,
      rx_bytes: 2048,
      txmsgs: 4,
      rxmsgs: 5,
      brokers: {
        one: { state: "UP" },
        two: { state: "DOWN" }
      },
      topics: {
        orders: {
          partitions: {
            0: { consumer_lag: 7 },
            1: { consumer_lag: -1 },
            2: { consumer_lag: 9 }
          }
        }
      }
    })
  });

  const output = await metrics.render();
  assert.match(output, /kafka_client_brokers\{role="consumer"\} 2/u);
  assert.match(output, /kafka_client_brokers_up\{role="consumer"\} 1/u);
  assert.match(output, /kafka_client_messages_queued\{role="consumer"\} 3/u);
  assert.match(output, /kafka_client_bytes_sent\{role="consumer"\} 1024/u);
  assert.match(output, /kafka_client_consumer_lag\{role="consumer"\} 16/u);
});
