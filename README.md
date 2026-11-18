# tsservicelib

TypeScript/Node.js implementation of ServiceLib. The Go implementation is the
semantic source of truth; this package preserves the same graph, lifecycle,
operator, configuration, transport and telemetry contracts using strict ESM
TypeScript.

## Requirements

- Node.js 24.19.0 LTS
- pnpm 11.18.0 through Corepack

## Development

```bash
corepack enable
corepack prepare pnpm@11.18.0 --activate
pnpm install --frozen-lockfile
make check
```

Production JavaScript and declaration files are emitted to `dist/`. The
package does not execute TypeScript directly in production.

## Concurrency model

One generated service owns one graph in one Node.js isolate. `TaskPool` and
`PriorityTaskPool` preserve the canonical asynchronous admission, ordering,
resize and shutdown semantics; they are not OS-thread pools and do not claim
parallel JavaScript execution merely because the container has multiple CPUs.

The framework deliberately does not duplicate a graph through `cluster` and
does not move graph nodes, callbacks or stream state into `worker_threads`.
Transport and other I/O waits use asynchronous Node APIs, so they do not block
the event loop. CPU-bound or synchronously blocking application work must be
isolated by the application or deployment without changing the framework's
single-graph contract.

## Background errors and shutdown

Default process pools and detached tasks without an explicit error handler forward
unexpected callback throws and rejected promises to Node's uncaught-error policy.
`ServiceApp` logs such failures and forwards them even if logging fails. Generated
executables stop and exit with a nonzero status rather than silently continuing.
An explicitly supplied `onError` handler owns error handling; Temporal records
failures in the workflow environment instead of applying the process policy.

An awaited `RuntimeTaskRegistry.admit()` failure remains a rejected promise for
its caller. Rejection with the actual cancellation reason (or `AbortError` after
signal cancellation) is ordinary cancellation, not an unhandled task failure.
Unrelated failures after cancellation are not suppressed. Typed business failures
sent through collectors or error branches are data, not uncaught exceptions.

Like Go, a pool's stop deadline reports slow shutdown but does not abandon its
accepted callbacks. The generated executable applies one shutdown budget across
the stop hook and runtime, and exits explicitly after shutdown or budget expiry.
Synchronous code that blocks the Node event loop also blocks in-process timers;
an external supervisor is required to enforce a hard deadline in that situation.

## gRPC request lifetime

All four source RPC modes reserve each active `streamId` until `EndRequest`
finishes, including requests without a result stream. Duplicate active IDs are
rejected per endpoint; independent IDs proceed, and completed IDs can be reused.
An active result callback finishes before `EndRequest`; retired callbacks reject
late delivery and release their stored callback references.

For unary sources, `ResultContext.done()` does not complete the RPC: the response
comes from `Sender.send()`. Streaming modes retain their own Done semantics.

## Service-local SubStream

SubStream makes a reusable graph callable from business code inside its owning
service. Generated services expose typed getters such as `getLookupSubStream()`.
Use custom makers and a narrow provider interface to inject a handle; no endpoint
or function-to-substream declaration is required. Handles can be captured during
construction, but invoked only after graph binding.

```typescript
import { SubStreamCollectorFunc } from "@gorundebug/tsservicelib/runtime/graph";

const results: string[] = [];
await service.getLookupSubStream().consume(
  context,
  "hello",
  new SubStreamCollectorFunc<string>((callerContext, result) => {
    results.push(result);
    return true; // Enough results; false continues collecting.
  })
);
```

The collector may return `boolean` or `Promise<boolean>` and receives the caller's
`MessageContext`. Preserve the supplied context in business emissions. Each
concurrent or nested invocation owns separate completion state without copying
the graph or adding a message ID argument; callbacks for one call are serialized.

The entry's `valueType` is its input type; `source` points to the reachable result
producer and supplies the output type. The entry has one body consumer; Split
can branch inside the body. There is no separate ResultStream or error port.

Completion drops late results, not running work. Use cancellation/deadlines if
the collector may never finish, and make callbacks cancellation-aware. Runtime
failures reject `consume`; business failures remain explicit result values or
ordinary error branches. Existing Join keys and pools are unchanged; a waiting
caller must not occupy all execution capacity its substream needs.

Temporal is supported through the workflow environment. Its custom workflow
makers receive the generated SubStream provider. Use runtime workflow-aware
delay/scheduling, deterministic business code and Activities for external I/O.
Replay reconstructs invocation state; a local non-workflow invocation is not
durable across a process restart merely because its body calls a Temporal Sink.

### Join expiry and callback failures

Join callbacks for one key are serialized; different keys remain independent.
An explicit context deadline controls expiry, including cancellation of that context.
Without a context deadline, the configured TTL controls expiry: cancelling the message
context alone does not shorten it. A pending expiry checks TTL renewal after an active
callback completes, and a callback cannot republish a group replaced while it was waiting.

An unexpected exception or rejection from a detached expiry callback follows the runtime's
unhandled background failure policy rather than being silently discarded. Genuine task
cancellation is distinguished from an unrelated failure. Typed business-error values are
ordinary graph data, not unhandled exceptions.

Stopping Join storage ends periodic rotation, not the lifetime of individual groups.
Accepted expiry callbacks remain scheduled. Like Go's rotation lock, stop waits for
in-flight TTL-bearing `joinValue` calls, but not zero-TTL calls or an expiry callback
already executing. New TTL-bearing calls wait behind that maintenance stop and may
continue afterward; admission of new graph requests belongs to the surrounding runtime.
The outer service shutdown deadline still bounds process lifetime.

### Native NATS Core and JetStream

The Node adapters use the official `@nats-io/transport-node` and
`@nats-io/jetstream` packages, both pinned to `3.5.0`. Connector kinds `7` (Core)
and `8` (JetStream) remain separate. Generic DataSource/DataSink contracts are
unchanged; the native entry points are:

```typescript
// @gorundebug/tsservicelib/datasource/nats
makeNATSEndpointConsumer<H, T, R, E>(
  stream: TypedInputStream<T, R, E>, handler: EndpointHandler<H, T, R, E>
): Consumer<T>;

// @gorundebug/tsservicelib/datasink/nats
makeNATSEndpointConsumer<H, T, E>(
  stream: TypedSinkStream<T, E>, handler: EndpointHandler<H, T, never, E>
): Consumer<T>;
makeNATSEndpointConsumerWithResult<H, T, R, E>(
  stream: TypedSinkStreamWithResult<T, R, E>, handler: EndpointHandler<H, T, R, E>
): Consumer<T>;
```

Source handlers receive real SDK `Msg` or `JsMsg` deliveries. Core has no ACK;
JetStream handlers must explicitly call native acknowledgement methods.
`ResultContext.done()` only completes local processing. Callback registration
must precede graph collection. Native headers carry stream identity and tracing;
local inherited context values remain available. A retained native message is
not protected by a portable stale-ACK fence.

Sink handlers fill `SinkMessage<R>.data: Uint8Array` and `.header: MsgHdrs`.
`send(context: MessageContext, callback: DeliveryCallback<R>): void` admits work
without waiting for the broker. The callback receives
`(delivery: Delivery | undefined, error: Error | undefined): R | undefined`;
a returned result is emitted to the result graph.
`sendSync(context: MessageContext): Promise<Delivery>` explicitly awaits the
native outcome. `Delivery.ack` is a native `PubAck` for JetStream and undefined
for Core, where completion means a protocol flush, not subscriber processing.
`out`/`skip` emit results without publication; `stream.errorCollect` retains typed
business errors. Sink EndRequest follows ConsumeMessage; admitted delivery
callbacks may finish later. Do not retain SinkMessage after ConsumeMessage.

Servers are required and accept only `nats://` or `tls://` URLs without embedded
credentials, paths, queries, or fragments. TLS URLs and TLS fields require
`tlsEnabled: true`, including mixed URL lists. Authentication supports typed
username/password, token, or mounted credentials-file configuration; TLS uses
mounted CA/certificate/key files. Existing environment resolution is preserved.
Timeouts use integer milliseconds, defaulting to 2000 for connection and 5000
for operations. The canonical maximum is `9223372036854`; Node SDK startup
explicitly rejects values above `2147483647` because native timers cannot
represent them. Independent `tlsServerName` overrides are unsupported: use that
hostname in each server URL.

Core endpoints use subject and optional source queueGroup. JetStream endpoints
require streamName and, for sources, durableName; queueGroup is forbidden.
Creation flags default to false. Explicit creation only creates missing resources;
existing streams and explicit-ACK pull consumers are validated, never reconfigured.
Sinks cannot configure consumer fields. There is no request/reply adapter.

Admission is bounded per endpoint. Core sources admit at most 256 pending
messages / 8 MiB plus the active message; overflow stops the subscription and
reports failure while draining admitted work. JetStream prefetch is one message
plus the active message and requires broker `max_payload <= 8 MiB`.
Sinks bound active handlers and publications to 256 each, with an 8 MiB
publication budget including native headers. Saturation rejects admission.
Native reconnect/resumption remains enabled. Ambiguous publication failures are
not retried by the adapter; their budget remains reserved until a native flush
or connection closure resolves buffered work. Stop first closes admission, drains
admitted work to its supplied deadline, then cancels/closes. JavaScript handlers
must cooperate with cancellation; shutdown cannot preempt synchronous user code.

Run the native integration tests with an installed NATS server:

```sh
pnpm build
pnpm exec tsc --build tsconfig.test.json
NATS_SERVER=/absolute/path/to/nats-server node --test --test-concurrency=1 dist-test/test/nats-config.test.js dist-test/test/nats-native.test.js
```

Native tests otherwise skip explicitly. Coverage includes ACK/redelivery, Core
and JetStream reconnect, queue groups, TLS/token authentication, typed results,
context propagation, bounded admission and shutdown deadlines. This checkout has
no TypeScript API-schema generation script (`src/api/index.ts` is a placeholder);
the existing build generates runtime declarations and distributable JavaScript.

### RabbitMQ AMQP 1.0

Supported sinks and experimental source probes use RabbitMQ's designated `rabbitmq-amqp-js-client`
1.0.0. Import `datasource/rabbitmq` or `datasink/rabbitmq`; typed native settings
are exported from `runtime/config`. Explicit accept/release/reject are separate
from Done; async publish callbacks report accepted/unroutable/rejected/unknown.
Shutdown drains within its supplied context, then terminates the owned SDK worker.

SDK 1.0.0 supports only the default virtual host through its public API and does
not expose receiver-credit control. Source `consumerCredits` bounds admission;
overflow closes the connection for unsettled redelivery. Sink bodies are bytes or
strings; publisher limits remain held through callback completion. Full profile,
configuration constraints and acceptance commands are in the repository's
`README.rabbitmq.md`. Generated-project integration is a separate gate.

Redis Streams native source/sink adapters use official node-redis with explicit
XACK, bounded asynchronous publication and standalone-primary validation.
See [the Redis Streams profile](README.redisstreams.md) for configuration,
delivery guarantees, public-client limits and disposable-broker acceptance.

### RabbitMQ source eligibility

RabbitMQ AMQP 1.0 **sinks remain supported**. The source adapter is experimental
capability-test code: official `rabbitmq-amqp-js-client` 1.0.0 has no supported
native receive-credit/pause API, and a local admission limit is not native flow
control. Builtin generation and authoring validation reject TypeScript RabbitMQ
sources, including mixed graphs. See [the current upstream audit](test/native-probes/rabbitmq-credit-current.md).
