# TypeScript concurrency model

The TypeScript runtime executes one service graph in one Node.js isolate. This
is the same asynchronous concurrency model used by the Python implementation:
tasks may overlap while they wait for I/O, but JavaScript callbacks do not run
in parallel on multiple CPU cores.

`TaskPool` and `PriorityTaskPool` are admission and ordering pools, not OS
thread pools. `executorsCount` is the maximum number of admitted callbacks
whose promises may be incomplete at the same time. Increasing the value starts
queued work immediately. Decreasing it never interrupts admitted work; it only
prevents new starts until the active count is below the new target. A pool stop
rejects new admission and drains all work that was already accepted.

Blocking calls and CPU-heavy loops must not run in these pools because they
block the service event loop regardless of `executorsCount`. Framework code
must use asynchronous Node APIs for network, filesystem and timer work.

The framework does not introduce a second worker-pool API that is absent from
the canonical runtimes. CPU-heavy business work must therefore be split into
asynchronous operations or moved behind an application-owned external service;
it is not silently given different graph/state ownership semantics in the
TypeScript implementation.

Container CPU quota therefore does not by itself make one service isolate use
all available cores. Horizontal replication is a deployment concern and does
not change the single-service runtime contract.
