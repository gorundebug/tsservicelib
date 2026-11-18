# Apache Pulsar native adapters

The source and sink use official `pulsar-client` **1.18.0**, without upstream
patches, worker bridges, or changes to the public DataSource/DataSink interfaces.
Import makers and handlers from `@gorundebug/tsservicelib/datasource/pulsar`
and `@gorundebug/tsservicelib/datasink/pulsar`. Runtime configuration is also
exported through the workflow-safe config entry point.

## Configuration and resources

Use connector type 11, implementation `pulsar-client-node`, one `servers` service
URL and `adminUrl`. Secure service URLs require HTTPS administration. Native TLS
verifies the CA and hostname; optional `tlsCertFile`/`tlsKeyFile` supply both
transport identity and official `AuthenticationTls`. Token authentication is
mutually exclusive with client identity. No insecure switch or hostname override
is supported. Credentials belong in runtime configuration.

An endpoint selects one fully qualified `persistent://tenant/namespace/topic`.
Sources require `subscriptionName`; modes are Shared, Exclusive or Failover.
`createTopic` and `createSubscription` default false. `partitions` is accepted only
with explicit topic creation; existing partition shape must match. Initial
position Earliest/Latest applies only when creating a missing subscription, never
resets its cursor. The adapter does not create tenants/namespaces or change their
policies. Startup requires explicitly disabled namespace auto-topic creation;
sources also require explicitly disabled auto-subscription creation. Inherited or
missing policy values are insufficient proof. Admin responses are bounded to 4 MiB.

## Delivery and lifecycle

Source `ConsumerMessage.message` is the official native Message (binary payload,
MessageId, key, properties and timestamps). `ack()` is individual native ACK;
`nack()` requests native negative acknowledgement. Neither handler return, Done,
nor graph results acknowledge a message. ACK completion is the native client's
completion, not proof of a broker-confirmed durable acknowledgement. Delivery
handles expire when their request ends. Closing never deletes a subscription.

The source keeps at most one outstanding **no-argument** native `receive()`.
That overload uses the native asynchronous API; the timeout overload is not used.
Cancelled local receive waits retain the same native request until it completes
or the client closes. Admitted handlers are bounded by receiverQueueSize
(default 256, maximum 4096), 1 MiB per message and 8 MiB of handler payloads.
The native receiver prefetch queue is separately count-bounded. Oversized incoming
messages are reported and left unacknowledged.

Set sink `message.message` to the official ProducerMessage shape with Buffer
`data`, string properties, optional partitionKey and eventTimestamp. Payloads and
properties are copied at admission. Unsupported ordering, explicit sequence,
replication and delayed-delivery options reject before native handoff. Native
batching and blocking-on-full are disabled. `send(context, callback)` returns
upon bounded admission; `sendSync(context)` explicitly awaits the same outcome.
There is no additional unbounded publication queue. PublisherMaxInFlight
(default 256, maximum 4096) and 8 MiB bound pending native sends independently of
callbacks. Cancelled waits retain native capacity until the native operation ends.
Callbacks and handlers also retain their own bounded capacity through completion.

Delivery contains `outcome`, optional native `id`, and optional `error`:

- NotSent: local validation/cancellation/capacity/closed rejection before handoff,
  or an exact pinned-client native rejection proving no publication.
- Accepted: the native send promise returned a MessageId.
- Rejected: an exact native authorization/schema/fencing/termination rejection.
- Unknown: timeout, cancellation or ambiguous connection/close failure after
  handoff. Unknown never means safe to retry.

The official Node client exposes send errors as strings, not a structured result
code. Only exact v1.18.0 strings are mapped; unknown strings remain Unknown.
The adapter never retries publication. The native client may reconnect and
retransmit pending messages; this is not an exactly-once claim.

ConsumeMessage return retires SinkMessage for new sends, including while an older
callback still owns its context. EndRequest runs after ConsumeMessage and may
precede delivery callbacks. State captured by callbacks must remain valid until
those callbacks finish; EndRequest is not publication completion.

Stop first prevents new input/admission, then drains admitted handlers/callbacks
while source ACK remains usable. Its deadline cancels owned request contexts and
starts one owned native close operation. Arbitrary user callback code cannot be
forcibly terminated; callbacks must cooperate with cancellation. Late native
startup/close is observed and repeated Stop can await the same cleanup. Runtime
start context cancellation propagates to endpoint requests. Native reconnection
is owned by the official client, without an adapter retry loop for publication.

Case-insensitive x-stream-id/x-trace/W3C metadata is extracted and inherited;
caller properties are retained except where inherited transport metadata replaces
the same logical key. Register result callbacks before synchronous graph collect.

## Acceptance and limits

Focused tests exercise config, native resources and partition shape, all three
subscription modes and cursor reuse, binary data, metadata, explicit ACK/NACK,
Done without ACK, result/error routes, async/sync/skip, retained handles, capacity,
handler/callback drain, deadline cancellation, partial startup, receipt loss,
native reconnection and native pending-byte ownership. Test-owned transparent
proxies forward real broker traffic and withhold real receipts for ambiguity.
TLS tests cover trusted transport, untrusted roots, trusted wrong-host rejection,
client identity and missing identity, plus HTTPS administration.

Run after building both TypeScript targets:

```sh
PULSAR_TEST_URL=pulsar://127.0.0.1:16650 \
PULSAR_TEST_ADMIN_URL=http://127.0.0.1:18080 \
node --test --enable-source-maps \
  dist-test/test/pulsar-config.test.js \
  dist-test/test/pulsar-native.test.js \
  test/native-probes/pulsar-adapter.test.mjs
```

Each native suite creates a unique test-owned namespace and disables automatic
creation only there. It deletes its own resources and never restarts the broker.
The focused command passes 54 checks (25 configuration, 6 public graph/lifecycle,
23 internal native/fault/outcome checks). Build, scoped lint, formatting and package
export checks also pass. Native acceptance here uses macOS arm64 Node 24.16.0. Parent-reported Linux eligibility also passes with official 1.18.0 on pinned
Node 24.19.0-bookworm-slim, arm64 and emulated amd64: native installation, resolved
shared libraries, binary/ID/ACK/NACK/redelivery, four pending asynchronous receives,
libuv/timer progress (8 ticks), and close (2.87/2.84 ms), exit 0. This is not full
generated Linux image or throughput acceptance. The parent owns those gates.

Primary references: [versioned public API](https://github.com/apache/pulsar-client-node/blob/v1.18.0/index.d.ts),
[native producer result handling](https://github.com/apache/pulsar-client-node/blob/v1.18.0/src/Producer.cc),
[native consumer implementation](https://github.com/apache/pulsar-client-node/blob/v1.18.0/src/Consumer.cc),
and [official client configuration](https://pulsar.apache.org/docs/client-libraries/node-configs/).

## Authenticated token ACL gate

The isolated official Pulsar 4.1.2 fixture enables token authentication **and**
authorization. It uses fresh HS256 signing material, one-hour test tokens,
loopback-only random ports, private temporary files, and a unique container.
Broker token and signing-key settings reference files so broker startup diagnostics
cannot print inline authentication values.
Cleanup removes only its container, anonymous volumes and secret files; the shared
broker is never restarted or modified. Override `PULSAR_AUTH_IMAGE` to use the
same official image from another registry.

```sh
PULSAR_AUTH_TEST=1 node --test test/native-probes/pulsar-auth.test.mjs
```

All 18 focused checks pass. They cover SDK admin-capable token publication/consumption and
ACK, official producer-only/consumer-only tokens, namespace and topic ACLs,
wrong-role and forbidden-resource rejection, missing/invalid HTTP credentials,
invalid native credentials, bounded native close, and secret-free errors/client
diagnostics/broker logs. Invalid native credentials can resolve as `TimeOut`
after the configured operation deadline, not necessarily AuthenticationError.

Runtime-only `adminToken` separates the REST identity from native `token`.
A nonempty override supplies every admin Bearer header; absent/empty preserves
legacy fallback to `token`. HTTP 401/403 never triggers credential fallback.
Use a tenant-admin role for preflight/provisioning and a producer/consumer-only
role for native connections. Namespace policy checks and opt-in creation remain
mandatory. The app still holds the tenant-admin secret; this is connection-identity
separation, not process privilege isolation. `adminToken` with TLS client certificate
identity is explicitly rejected in this initial token/token profile. CA-only TLS
with token credentials remains supported. CR/LF in the override is rejected.

The extended isolated suite passes 28 checks, including Go race/config and Python
native/config subprocess acceptance against the same fixture. It proves scoped
native denial despite valid tenant-admin preflight, invalid admin credentials
without fallback, secret-safe diagnostics and bounded closure in all three SDKs.
Run from tsservicelib with `PULSAR_AUTH_TEST=1 PULSAR_AUTH_CROSS_LANGUAGE=1 node
--test test/native-probes/pulsar-auth.test.mjs`. `PULSAR_TEST_GO` can select the Go
executable; Python uses the sibling pyservicelib virtual environment. Runtime
configuration shapes are Go `AdminToken`, Python `admin_token` (alias `adminToken`),
and TS optional `adminToken`. Keep this secret out of canonical topology/API
projection; generator/environment binding is owned separately.

For reuse from Go/Python, run
`node test/native-probes/pulsar-auth-fixture.mjs`. It prints only the path to an
owner-readable manifest containing service/admin URLs and tokens. Keep that process
alive while testing; SIGTERM/SIGINT cleans up the fixture. Do not print the manifest
or commit its contents. The test suite provisions its ACL topology using the admin
API; the standalone fixture intentionally starts with no application topology.

References: Apache [token administration](https://pulsar.apache.org/docs/4.0.x/security-token-admin/)
and [authorization model](https://pulsar.apache.org/docs/4.0.x/security-authorization/).
