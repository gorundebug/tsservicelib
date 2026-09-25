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
