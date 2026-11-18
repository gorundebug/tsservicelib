# Native Kafka commit observations

The official `@confluentinc/kafka-javascript` 1.9.0 native API reports committed
offsets through `offset_commit_cb`, without an individual request ID. SA matches
topic/partition/next offset. A scoped callback error now rejects only matching
waiters; unscoped errors remain ambiguous failures for pending operations. After
a successful observation SA rechecks the assignment lease, so a receipt arriving
after revoke cannot make an old handle successful.

This is an **observed position**, not proof of a particular submitted request.
Automatic and explicit commits to the same position cannot be distinguished;
one native callback can satisfy multiple same-position waiters. No monotonicity
or individual-record ACK guarantee follows. Callers must order retained-handle
commits themselves. A stale/failed waiter cannot undo an admitted native commit.

Do not replace this with `commitCb`: in the official 1.9.0 source,
`lib/kafka-consumer.js::commitCb` invokes `KafkaConsumerCommitCb::Execute` in
`src/workers.cc`, which calls `KafkaConsumer::Commit` in `src/kafka-consumer.cc`.
That method uses `commitAsync`; the callback confirms admission, not the broker
receipt. `commitSync` would block the Node thread and is not an acceptable fix.
No upstream patch, nonce hidden in user metadata, or custom native bridge is used.

`test/native-probes/kafka-commit-receipts.mjs` tests the real SA adapter with a
controlled native boundary: unrelated-partition errors, revoke-before-receipt,
unrelated auto-commit positions, and the explicit same-position limitation. It
is registered by `test/kafka-commit-receipts.test.ts`. Four cases pass:
`/private/tmp/sa-ts-kafka-receipts.log`. These are not live broker fault tests.

The native checkpoint/rebalance suite is run against a disposable broker with
the current compiled SDK mounted into the cached official-client test image:

```sh
node node_modules/typescript/bin/tsc --build tsconfig.test.json
python3 test/run-kafka-checkpoint-native.py --artifacts /private/tmp/sa-ts-kafka-receipts-native
```

The image uses Node 24.19.0 and the official native client. Three native tests
pass (none skipped): `/private/tmp/sa-ts-kafka-receipts-native/acceptance.log`.
Its checkpoint tests
do not simulate two distinct wire requests with identical offsets or a targeted
commit-receipt network fault. Exact per-request receipt identity remains
unsupported by this adapter; it must not be advertised as Rust-equivalent.
