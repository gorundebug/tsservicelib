# Redis Streams / TypeScript

Native standalone Redis Streams uses official `redis` (node-redis) **6.3.1**,
pinned in the lockfile. Redis 7.2+ standalone primary is required. Startup checks
INFO server and ROLE; Cluster, Sentinel, replicas and multiple URLs are rejected.
No SDK patches or generic DataSource/DataSink changes are required.

## Public contract

- `@gorundebug/tsservicelib/datasource/redisstreams` exports
  `makeRedisStreamsEndpointConsumer<H,T,R,E>(stream, handler)`.
- `@gorundebug/tsservicelib/datasink/redisstreams` exports
  `makeRedisStreamsEndpointConsumer<H,T,E>(stream, handler)` and
  `makeRedisStreamsEndpointConsumerWithResult<H,T,R,E>(stream, handler)`.
- `RedisStreamsDataConnectorConfig` and `RedisStreamsEndpointConfig` are exported
  through runtime/config. Backend protocol identity is `redisstreams`; type is
  10 and implementation is `node-redis`.

Source ConsumerMessage.message exposes native entry `id`, ordered binary `fields`
(including duplicate names), `reclaimed` and retained `bytes`. `ack()` explicitly
sends XACK and returns the server's affected PEL entry count. ResultContext.done()
only completes local work. Register result callbacks before collect: result
routing may be synchronous. Begin failure skips Consume/End; successful Begin
receives exactly one End after actual handler/result work. Transport failures go
to onError without inventing graph E values or ACKing input.

ACK handles expire on request completion or connection replacement. This local
fence is **not Redis ownership fencing**: XACK identifies stream/group/ID and can
acknowledge an entry claimed by another consumer. Choose claimIdleMs above normal
processing latency and make business effects idempotent. ACK does not delete a
stream entry. Failed or omitted ACK leaves work pending for explicit reclaim.
Deleted PEL references reported by XAUTOCLAIM are observable onError diagnostics.

SinkMessage.fields accepts ordered string/Uint8Array field/value pairs; there is
no implicit JSON codec. send(context, callback) uses genuine asynchronous native
XADD and maps its possibly asynchronous callback to typed R. sendSync awaits the
same native operation. EndRequest follows ConsumeMessage return, not delivery;
callbacks may outlive EndRequest, so captured state must remain valid. The
callback retains admission capacity until it finishes. Existing errorCollect,
out and skip preserve the ordinary typed graph routes.

Delivery outcomes:

| outcome   | guarantee                                                       |
| --------- | --------------------------------------------------------------- |
| published | XADD returned the native entry ID in `id`                       |
| rejected  | Redis error reply or NOMKSTREAM nil; no successful append       |
| not-sent  | cancellation/unavailability before native admission             |
| unknown   | reply lost, timeout or cancellation after possible transmission |

Unknown XADD is never automatically retried. A published result proves a primary
accepted XADD, not replication, disk persistence, downstream consumption or
exactly-once effects. Canonical outcome mapping is Accepted = outcome ===
`published`, EntryID = id; EventID and Reason remain application mappings.

Native fields retain caller data and duplicates. Case-insensitive x-stream-id,
x-trace, traceparent, tracestate and baggage carry inherited context; context
metadata replaces only matching transport fields in an owned publication copy.

## Configuration

One `redis://host:port` or `rediss://host:port` URL defaults an omitted port to 6379,
without inline credentials, database path, query or fragment. redisDatabase
(default 0) selects the database; runtime username/password supports ACL and
password-only default-user authentication. Token and credentialsFile are rejected.
TLS verifies peers, with optional CA, certificate/key pair and server-name override.
Connect/operation timeouts default to 2000/5000ms; explicit values are integers
1..2147483647ms. Credentials belong in runtime bindings, never topology URLs.

Source requires exact literal streamKey and consumerGroup. consumerName defaults
to a unique endpoint-instance identity. createConsumerGroup defaults false;
opt-in XGROUP CREATE MKSTREAM uses groupStartId (default `0-0`, or `$`).
BUSYGROUP preserves existing group position. groupStartId requires creation opt-in.
Source defaults: readCount 64 (1..4096), readBlockMs 1000 (1..60000), claimIdleMs
60000 and claimIntervalMs 1000 (positive integer milliseconds <=2147483647).

Sink requires streamKey. createStream defaults false, issuing XADD NOMKSTREAM;
true permits XADD to create the stream. publisherMaxInFlight defaults 256,
range 1..4096. Source-only options on sinks and sink-only options on sources are
rejected even when explicitly false. There is no implicit trim, TTL, deletion,
consumer-group reset, dead-letter queue or business retry policy.

ACLs must permit INFO/ROLE plus TYPE for sink startup or XINFO GROUPS for source
startup, XREADGROUP/XAUTOCLAIM/XACK for receiving, XADD for publishing, and
XGROUP CREATE only when creation is opted in.

## Concurrency, recovery and shutdown

A source has a dedicated blocking-read connection and a separate connection for
ACK, management and reclaim. readCount bounds active concurrent handlers as well
as requested entries. XAUTOCLAIM retains its scan cursor across empty pages and
returns to zero only when Redis says the scan completed. Processing order is not
guaranteed. Local retained admission is capped at 8MiB, with 1MiB per entry;
oversized input is reported and left pending, never sent into the graph as empty
payload. Sink admission is bounded by publisherMaxInFlight and the same byte cap.

**Public client limitation:** COUNT limits entries, not bytes. node-redis decodes
native replies before adapter checks; a huge Redis entry can allocate more than
the retained-work limit. These bounds are not a total-process heap guarantee.

Native offline queues and automatic reconnect/replay are disabled. The adapter
replaces failed clients with at most 60 recovery attempts, 250ms apart, using
fresh connection deadlines and the lifetime cancellation signal. Revalidation
never resets existing group position. Old settlement handles are fenced; pending
source work can be reclaimed. Unknown publications are not replayed.

Stop closes input admission and the blocking reader first, preserving the ACK
connection while admitted handlers drain. Sink Stop drains publications and
callbacks. The supplied stop context bounds waiting; expiry cancels request
contexts and destroys owned native clients through the public API. Application
callbacks ignoring cancellation may outlive Stop; EndRequest does not race an
active source callback. No additional worker thread or unbounded delivery queue
is used.

## Acceptance

Compile with `./node_modules/.bin/tsc --build tsconfig.test.json`, then run:

```sh
REDIS_NATIVE=1 node --test --enable-source-maps --test-timeout=60000 \
  dist-test/test/redis-native.test.js dist-test/test/redis-config.test.js
```

The fixture creates and removes its own Redis 7.4 Alpine container on a random
loopback port. It never restarts a shared broker. Docker and OpenSSL are needed;
native tests skip without REDIS_NATIVE=1. Tests exercise binary duplicates,
explicit ACK/Done, empty reclaim pages/deleted PEL references, admission/drain,
TLS/auth/database selection, reconnect fencing, unknown/no replay and typed R/E.
Generated canonical-service acceptance remains a separate parent-owned gate.

Primary sources: [official node-redis documentation](https://redis.io/docs/latest/develop/clients/nodejs/),
[6.3.1 release](https://github.com/redis/node-redis/releases/tag/redis%406.3.1),
[XACK](https://redis.io/docs/latest/commands/xack/),
[XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/).

Local evidence (2026-11-17): 54 focused tests passed, including 17 native Redis
scenarios. The completed SDK regression run passed 585 tests with 53 optional
integration cases skipped and zero failures. Runs used Node 24.16.0 and disposable
Redis 7.4; the shared package engine target remains Node 24.19.0. That exact-version
and generated-service gate is not claimed here. TypeScript build, focused lint,
format, package/export and structure checks also passed.

To reuse the shared acceptance broker, set
`REDIS_TEST_URL=redis://127.0.0.1:16379` alongside `REDIS_NATIVE=1` (fixture password
`adapter-test`). Shared mode never creates, restarts or removes a container and
skips the server-role mutation test. It uses unique keys and removes only its
recorded keys from databases 0 and 1. The replica-rejection test remains covered
by disposable mode. Shared-broker acceptance passed 53 tests, including 16 native
scenarios, with this one intentional skip and zero failures (2026-11-17).
