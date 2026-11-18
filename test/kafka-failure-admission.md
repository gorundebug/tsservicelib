# Kafka request failure admission

Begin, consume, end and result-callback failures propagate to the native partition
lane after request finalization. A result callback that throws after Done remains
a failure; its original result-stream error route is preserved. The private result
object wakes the waiting request without presenting that wake as successful Done.
No public handler or DataSource/DataSink method was added.

The native adapter leaves the failed assignment paused and aborts its lease.
Retained marks/commits reject, buffered successors are not dispatched, and healthy
partitions keep running. Recovery requires reassignment or restart; the adapter
does not choose a retry or poison-message policy. An ACK explicitly issued before
failure cannot be undone. Native commits already admitted may have unknown outcomes.

Controlled endpoint tests cover begin/consume/end and delayed callback exceptions,
including Done-then-throw, exactly-once finalization and no implicit ACK. The
native two-partition scenarios cover ordinary and restored positions, a buffered
batch of 40 records, post-failure traffic, retained handle fencing and broker
offset inspection. An absent committed partition is distinct from offset zero
and is valid evidence that no group offset has been recorded.

Run the complete native gate with:

```sh
python3 test/run-kafka-checkpoint-native.py --artifacts /tmp/sa-ts-kafka-failure
```

The runner uses the pinned Node image, an isolated disposable Kafka container and
the current built dist/dist-test. It removes its own resources. For an isolated
fixture correction, `--test-name-pattern 'native failed partition'` runs only
the two new scenarios. A filtered run does not establish the other gate cases.

Evidence (2026-11-18): 29 local test entries pass in
`/private/tmp/sa-ts-kafka-failure-local.log`. The three pre-existing native cases
passed in `/private/tmp/sa-ts-kafka-failure-native/acceptance.log`; the two new
cases failed only because the test assumed an uncommitted partition was present
in the offset response. After fixing that assertion, both pass in
`/private/tmp/sa-ts-kafka-failure-native-corrected/acceptance.log` (7.064 seconds).
Runtime implementation was unchanged between those native runs.
