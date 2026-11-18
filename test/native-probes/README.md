# Official Pulsar Node suitability gate

This standalone probe precedes the SA adapter. It does not register a supported
transport or prove generated-service, TLS/auth, performance or failure acceptance.

```sh
npm install --prefix /tmp/sa-pulsar-node-probe --no-audit --no-fund --save-exact pulsar-client@1.18.0
PULSAR_NODE_MODULE=/tmp/sa-pulsar-node-probe/node_modules/pulsar-client \
PULSAR_TEST_ADMIN_URL=http://127.0.0.1:18080 \
PULSAR_TEST_URL=pulsar://127.0.0.1:16650 \
node test/native-probes/pulsar.mjs
```

Use an isolated Pulsar 4.1.2 broker with the public tenant. The probe creates a
unique namespace, disables automatic creation, explicitly creates its topic and
durable subscription, then removes only these resources. A 40-second watchdog
fails the process if a native operation does not terminate. This watchdog is test
supervision, not an SDK cancellation implementation.

Observed on macOS arm64, Node 24.16.0, official pulsar-client 1.18.0:
binary payload and MessageId/key/properties preserved; unacknowledged delivery
reappears after consumer replacement; NACK redelivers; individual ACK completes.
Four simultaneous pending receives allow a libuv PBKDF2 task and nine event-loop
timer ticks to finish. Consumer close rejects all four outstanding receives in
about 3.6 ms, and the process exits naturally after producer/client cleanup.
This is one functional measurement, not a latency bound or load benchmark.

The pinned official [Consumer implementation](https://github.com/apache/pulsar-client-node/blob/v1.18.0/src/Consumer.cc)
uses `pulsar_consumer_receive_async` for `receive()` without arguments. Supplying
a timeout switches to a blocking native call in a libuv AsyncWorker. Use the
first path in SA, with endpoint-owned consumer close for shutdown; do not add a
worker bridge or repeatedly abandon timed-out receive Promises. The official
[Producer implementation](https://github.com/apache/pulsar-client-node/blob/v1.18.0/src/Producer.cc)
uses native asynchronous send/close callbacks. Their rejection errors expose
text rather than structured Pulsar result enums; outcome classification needs a
versioned adapter and tests, not generic substring guessing.

Remaining gates: partial-start cancellation/cleanup, unavailable broker and send
ambiguity, admission/retained callback lifetime, TLS/credentials, generated paths,
Linux amd64/arm64 native package loading and the pinned project Node version.
