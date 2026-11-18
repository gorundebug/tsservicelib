# RabbitMQ AMQP 1.0 / Node TypeScript

> **Supported direction: sink only.** Builtin authoring and generation reject
> TypeScript RabbitMQ sources because the released official client lacks native
> receive-credit and pause/drain control. The source API and native tests below
> remain experimental suitability evidence, not supported application guidance.
> See [current upstream evidence](test/native-probes/rabbitmq-credit-current.md).


This native adapter uses RabbitMQ's designated `rabbitmq-amqp-js-client` **1.0.0**
(ISC), not amqplib/AMQP 0-9-1. The lockfile fixes the SDK's Git-hosted Rhea
transitive dependency at `d7474da72add1470be23a974ff670cff3d995e7f` (Apache-2.0).
Use the checked-in lockfile; refreshing that Git dependency is a separate change.
The published SDK bundles its runtime code; no dependency or SDK source is patched.

## Public entry points

- `@gorundebug/tsservicelib/datasource/rabbitmq` exports
  `makeRabbitMQEndpointConsumer<H,T,R,E>(stream, handler)`.
- `@gorundebug/tsservicelib/datasink/rabbitmq` exports
  `makeRabbitMQEndpointConsumer<H,T,E>(stream, handler)` and
  `makeRabbitMQEndpointConsumerWithResult<H,T,R,E>(stream, handler)`.
- `RabbitMQDataConnectorConfig` and `RabbitMQEndpointConfig` are exported by
  `runtime/config`. Generic DataSource/DataSink and graph interfaces are unchanged.

Handler signatures follow the existing typed TS native adapters. Source callbacks
must be registered before `collect`, which may synchronously return a result.
`ResultContext.done()` completes local work only. It never settles a delivery.
Source `ConsumerMessage.message` is an owned snapshot of the native SDK message;
`accept()`, `release()` and `reject()` explicitly request native settlement.
These are **local SDK settlement requests**, not broker-confirmed acknowledgments.
Handles expire on request completion or connection replacement. Begin failure
skips Consume/End; Consume failure reaches exactly one EndRequest. Transport
failures go to OnError, without synthesizing E or acknowledging input.

Sink `SinkMessage.message` accepts native message properties and a Buffer,
Uint8Array, or string body. `send(context, callback)` returns after bounded
admission and invokes the possibly asynchronous callback with a typed Delivery.
Its mapped R follows the ordinary result route. `sendSync(context)` awaits the
same outcome and does not implicitly emit R. `out`/`skip` are explicit local
result operations. Handlers route E through `errorCollect` as usual.

Delivery outcomes are `accepted`, `unroutable` (native RELEASED), `rejected`, and
`unknown`. `nativeOutcome` retains the SDK outcome name. For rejection,
`remoteState.condition` and `remoteState.description` retain the public native
error fields. Live Delivery objects and codec functions never cross owners.
A timeout/disconnect/cancellation after admission is conservatively unknown;
unknown publications are never automatically replayed. SDK 1.0.0 does not report
MODIFIED through its publish Promise; an unresolved operation times out unknown.

EndRequest follows ConsumeMessage return, **not publication confirmation**.
Async callbacks may outlive EndRequest; captured handler state must remain valid.
Their capacity reservation remains held until callback/result completion.

## Bounds, ownership and lifecycle

Each endpoint has one owned Node worker thread. The official SDK's publish API
is genuinely asynchronous and multiple native sends may be in flight. The worker
is needed because the public SDK cannot cancel a stalled connection attempt or
force-close transport. Terminating that owner enforces shutdown/startup deadlines
without private fields, monkeypatching, or a replacement AMQP client.

Publisher in-flight and source admission defaults are 256; limits are strict
integers 1..4096. A retained message snapshot is limited to 1MiB and each admission
boundary to 8MiB. Publisher reservations cover callbacks as well as native sends.
Source active requests and unsettled native handles are independently bounded;
handlers can run concurrently up to the admission limit, so ordering is not
promised. Native frame decoding happens before admission; these are retained-work
bounds, not a total-process heap limit.

**Receiver-credit limitation:** SDK 1.0.0 exposes neither receiver credit nor
pause/drain. `consumerCredits` therefore bounds adapter admission/retained native
handles, not negotiated wire credit. Its underlying receiver uses its automatic
credit window. On saturation the adapter reports overflow, closes the connection,
and leaves input unsettled for redelivery. It does not silently drop/ACK input or
claim adjustable wire credit. `Done` without settlement retains native capacity.
During Stop, unadmitted deliveries remain unsettled; overflow during drain may
close the generation and fail its remaining settlements rather than exceed bounds.

Stop first closes local admission and drains admitted handlers, result callbacks,
settlements and publications until its context deadline. At expiry it cancels
request contexts and terminates the worker. An application callback ignoring
cancellation can outlive Stop; EndRequest does not race an active source callback.
Workers and finite termination cleanup remain owned. The source keeps at most one
blocked OnError reporter. Recovery replaces workers, rotates configured servers,
and fences old delivery handles, with at most 60 attempts and 250ms backoff.
The startup context is not reused as the reconnect operation deadline; lifecycle
cancellation still closes the owner. A disconnected native connection is detected
through the public `isOpen()` API and pending-operation deadlines.

## Configuration and supported profile

Native type is 9, implementation `rabbitmq-amqp-js-client`. Fields match the shared
schema: servers, virtualHost, connectTimeoutMs/operationTimeoutMs,
username/password, TLS fields; endpoint queueName/queueType,
exchangeName/exchangeType/routingKey, createQueue/createExchange/createBinding,
consumerCredits/publisherMaxInFlight and standard identity/function metadata.

- URLs are amqp/amqps with no credentials, path, query, fragment, whitespace,
  controls or invalid port. Runtime username/password must be paired. Token and
  credentialsFile fail validation. TLS verifies peers; custom CA and client
  certificate/key files are supported.
- Timeouts default to 2000/5000ms and are limited to 1..2147483647ms by Node timers.
- **Only default virtual host `/` is supported by this SDK's public API.** Other
  vhosts fail validation. A TLS server-name override must match the URL hostname.
- Sources require queueName; sinks require queueName or exchangeName. Creation
  flags independently default false. Existing queues are read/validated without
  creation. Explicit creation defaults to durable quorum; quorum/classic are
  supported. Existing-only exchange validation uses native sender attach.
- Exchange types direct/topic/fanout require createExchange because the SDK has
  no read-only exchange-type inspector. Bindings are explicit opt-ins. Shutdown
  never deletes topology. URI delimiters/controls in SDK resource addresses are
  rejected because this SDK does not escape those path components.
- Sink bodies use the SDK's AMQP-value representation (binary for Buffer, string
  for text). Source snapshots also expose AMQP-data sections via their native
  `body.content` representation. Generated serdes must account for both forms;
  no JSON intermediary is used. Custom codec objects/functions and arbitrary
  AMQP structured sink bodies are outside this worker profile and fail admission.
- Native IDs, correlation IDs, properties and ordinary annotations are retained.
  AMQP application properties carry case-insensitive x-stream-id, x-trace,
  traceparent, tracestate and baggage. Caller properties remain intact; publication
  snapshots do not alias later caller mutations.

## Acceptance

From tsservicelib, after compiling `tsconfig.test.json`:

```sh
RABBITMQ_TEST_URL=amqp://127.0.0.1:15672 \
node --test --enable-source-maps dist-test/test/rabbitmq-native.test.js \
  dist-test/test/rabbitmq-config.test.js dist-test/test/nats-config.test.js
```

The fixture uses adapter/adapter-test and unique disposable queues/exchanges on
the existing RabbitMQ 4.3 acceptance broker. Fault cases use adapter-owned TCP/TLS
proxies, never restart/pause the broker. Tests cover outcomes, unknown/no replay,
settlement/redelivery, callback registration, typed R/E routes, backpressure,
startup cancellation/auth, TLS trust, reconnect fencing, and bounded drain.
The local TLS fixture needs OpenSSL. Broker tests skip without the URL.
Generated-project compilation and cross-language interoperability are separate
parent-owned gates; neither is claimed by SDK acceptance.

Primary sources: [RabbitMQ's client designation](https://www.rabbitmq.com/client-libraries/amqp-client-libraries),
[official designated SDK](https://github.com/coders51/rabbitmq-amqp-js-client),
[registry release](https://www.npmjs.com/package/rabbitmq-amqp-js-client/v/1.0.0).

Local acceptance evidence (2026-11-17): 68 focused tests passed, including 16
native RabbitMQ scenarios; complete compiled suite 564 passed, 20 skipped, zero
failures. TypeScript build/typecheck, focused ESLint/Prettier, package structure
and export checks passed. Runs used the installed Node 24.16.0 and RabbitMQ 4.3.0.
The package's shared engine target is 24.19.0; that exact-version generated-service
gate remains with parent integration. Frozen-lockfile installation passed. The workspace now applies the narrow
`rabbitmq-amqp-js-client>rhea: 3.0.5` override. This resolves the official SDK's
Git-hosted dependency to the official npm release without disabling pnpm exotic
subdependency or supply-chain checks. RabbitMQ SDK 1.0.0 remains unchanged.
Registry metadata identifies rhea 3.0.5 gitHead
`9ff8487f75073955e42309b43bd5b29ede72e55c`; this differs from the earlier Git tarball
revision and is covered by fresh acceptance: 46 RabbitMQ config/native checks pass,
including TLS, reconnect, lost confirmation, settlement, and bounded drain. The
lock records the npm release integrity; there are no upstream patches.
