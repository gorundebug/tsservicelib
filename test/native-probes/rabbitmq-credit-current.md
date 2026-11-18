# RabbitMQ TypeScript native credit suitability — current upstream audit

Checked 2026-11-18 against live primary registry/API responses, not just the
installed dependency or search-engine cache. **No eligible released public API
satisfies the required bounded-credit AMQP 1.0 source profile.** Recommend treating
that source profile as unsupported pending an upstream release with the required
capability and native acceptance. This finding does not change product capability
flags, generated code, the shared plan, dependencies, or existing source/sink code.
The accepted cancellation-lifetime fixes remain intact. Sink suitability is a
separate question; this audit does not invalidate native publication outcomes.

## Release and current development evidence

| Check | Live result |
|---|---|
| [npm package metadata](https://registry.npmjs.org/rabbitmq-amqp-js-client) | Only dist-tag is `latest: 1.0.0`; published 2026-06-25T15:06:33.604Z |
| [Upstream releases](https://github.com/coders51/rabbitmq-amqp-js-client/releases) | Latest stable v1.0.0; no newer release/pre-release in the retrieved list |
| Release commit | `c75bd5f2bc50d49e4991a9741f5a4378e364548f` |
| Current main HEAD | `5089d0234d9a40de544724738ffeec5e9fcd13a1`, 2026-11-12T09:57:43Z; virtual-host support |
| Consumer source comparison | Installed, release and current HEAD are byte-identical; SHA256 `7e4542681da5c75c7d72aa0c2ac2773aab9e402c045d166957338333f43e1011` |

Current source:
[consumer.ts](https://github.com/coders51/rabbitmq-amqp-js-client/blob/5089d0234d9a40de544724738ffeec5e9fcd13a1/src/consumer.ts),
[connection.ts](https://github.com/coders51/rabbitmq-amqp-js-client/blob/5089d0234d9a40de544724738ffeec5e9fcd13a1/src/connection.ts),
[environment.ts](https://github.com/coders51/rabbitmq-amqp-js-client/blob/5089d0234d9a40de544724738ffeec5e9fcd13a1/src/environment.ts).

The source options accept queue/stream selection, pre-settlement and a callback.
The public Consumer surface is start/close/id/replyTo; no credit, pause, resume or
unsettled-count operation exists. Connection/Environment options provide no escape
hatch for those settings. The callback returns void and its result is not awaited.
The underlying receiver is private. `start()` installs a listener; it does not
implement a credit window. `close()` closes the link, so closing/reopening at each
local capacity boundary would invalidate in-flight settlement rather than provide
a pause-and-drain mechanism.

## Direct upstream acknowledgement of the missing features

- [PR #94: initial credits](https://github.com/coders51/rabbitmq-amqp-js-client/pull/94)
  was **open, not draft, not merged** at retrieval; `merged_at=null`. Proposed head
  `40f7025925ebb3d571093adfa86969a34d9593ff`. Its proposal disables arrival-based
  replenishment and adds settlement-based credit restoration. This is not a
  released or current-main API and was not installed, built, patched or adopted.
- [Issue #34: graceful consumer shutdown](https://github.com/coders51/rabbitmq-amqp-js-client/issues/34)
  remains open. It requests pause, unsettled-count observation and close.
- The current [official multi-language guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries#consumption)
  describes graceful pause generically, but the JavaScript graceful-shutdown
  example explicitly says `CURRENTLY NOT IMPLEMENTED`. Java/.NET examples are not
  evidence that the JavaScript client implements their APIs.

## Why consumerCredits is not native credit

The adapter's `src/internal/rabbitmq/worker.ts` caps its local unsettled map using
consumerCredits. It cannot pass that bound to the current official client.
Installed rhea 3.0.5 `lib/link.js:344` defaults its receiver credit window to 1000;
lines 382–387 subscribe automatic replenishment to message arrival and receiver
open. The FlowController at lines 24–31 restores credit based on remaining credit,
not completion of application settlement. Thus even a 1000-entry application
buffer would not provide a total-unsettled bound. Increasing limits is not a fix.

This is read-only examination of the official client's selected dependency;
no direct-rhea transport or private receiver manipulation is proposed.

Retained native evidence already demonstrates the consequence:
`/private/tmp/sa-ts-source-lifetime-final-rabbit.log` — four focused cases passed,
including the deliberately saturated source. That case holds the first request
with consumerCredits=1, sends a second record, observes overflow, verifies both
records remain on the broker without ACK, and verifies bounded Stop. It proves
fail-closed local admission, not functioning native backpressure.

The rebuilt generated 5,000-record workload passed in
`/private/tmp/sa-ts-source-lifetime-performance-final/results.json`, but an earlier
same-profile run overflowed. Neither a passing low-in-flight sample nor closing the
connection on overflow meets the full native-credit source requirement.

## Alternatives checked against the chosen matrix

| Alternative | Why it does not resolve this profile |
|---|---|
| Current official JS AMQP 1.0 HEAD | Consumer API/source identical to stable release |
| Unmerged PR #94 | Not an eligible release; adopting it would bypass the no-upstream-patch constraint; also needs pause/drain review |
| Direct rhea | Different transport boundary, outside the designated-client choice; forbidden as a hidden replacement |
| [RabbitMQ Stream JS creditPolicy](https://github.com/coders51/rabbitmq-stream-js-client#custom-policy) | Real chunk credit control, but a different binary stream protocol and offset semantics; not individual queue accepted/released/rejected settlement |
| AMQP 0-9-1 Node libraries | Different unselected protocol/client eligibility; general inventory listing is insufficient designation under this matrix |
| Generic docs' Java/.NET APIs | Not APIs exposed by the designated Node implementation |

The authoritative local matrix is `servicegen/docs/message-brokers-clients.md`,
RabbitMQ designation and single-initial-profile sections. It selects AMQP 1.0
explicit queue settlement, not an interchangeable RabbitMQ wire protocol.
No allowed documented alternative was established. The shared matrix is not
edited by this audit.

## Exact requirement to reopen eligibility

A supported official release must expose finite native credits with replenishment
bound to settlement (not arrival), plus a supported way to stop new delivery and
drain already admitted settlements without closing their link prematurely. Verify
initial/replenishment bounds, duplicate settlement, reconnect reset and stale
handles, in-flight pause, stop deadline and failure ownership. Then pin that
release and rerun native saturation/reconnect/lifecycle and the unchanged generated
workload. A PR or a feature claim alone is insufficient acceptance.

## Retained evidence and scope

Machine summary: `rabbitmq-credit-current.json` beside this document. Full raw
registry/release/tag/HEAD/API/issue/PR responses and immutable source files:
`/private/tmp/sa-ts-rabbit-current-credit-audit/`, with `SHA256SUMS` and `summary.json`.
Live responses also record the package tarball integrity and current proposal head.

Only these standalone findings documents were added in this task. No dependency,
lockfile, SDK, capability, helper, broker or proxy changes were made. No new native
workload was run because no usable upgrade/API was found; retained saturation and
generated evidence above remains the applicable baseline.

## Product direction gate follow-up

The builtin TypeScript pack now declares RabbitMQ `features.source.supported=false`
and `features.sink.supported=true`. Generator, typed DSL/MCP and Designer expose
the source diagnostic before supported generation. Existing native source code and
tests are experimental capability evidence, not a supported product source. Sink
exports and delivery semantics remain supported. Historical whole-graph examples
do not override this direction restriction.
