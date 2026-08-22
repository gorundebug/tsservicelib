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
