import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import type * as ConfluentKafka from "@confluentinc/kafka-javascript";
import {
  ConfluentKafkaClientFactory, KafkaCheckpointError,
  type CheckpointOffsets, type KafkaConsumer, type KafkaConsumerControl, type KafkaRecord
} from "@gorundebug/tsservicelib/datasource/kafka";
import { Context } from "@gorundebug/tsservicelib/runtime";

const brokers = process.env["SA_TEST_KAFKA_BROKERS"];
const options = { skip: brokers === undefined ? "set SA_TEST_KAFKA_BROKERS for native acceptance" : false, timeout: 120_000 };

async function until(predicate: () => boolean, timeoutMs = 20_000): Promise<void> {
  const end = performance.now() + timeoutMs;
  while (!predicate()) {
    assert.ok(performance.now() < end, "Kafka checkpoint condition did not complete");
    await delay(10);
  }
}

async function close(consumer: KafkaConsumer, owner: Context): Promise<void> {
  await consumer.stop(owner.bounded(5000)).catch(() => {});
  await consumer.disconnect();
}

await test("native checkpoint rewinds/advances committed group and fails closed", options, async () => {
  assert.ok(brokers);
  const module = createRequire(import.meta.url)("@confluentinc/kafka-javascript") as typeof ConfluentKafka;
  const kafka = new module.KafkaJS.Kafka({ kafkaJS: { brokers: brokers.split(",") } });
  const admin = kafka.admin();
  const producer = kafka.producer();
  const topic = `sa-ts-checkpoint-${randomUUID()}`;
  const owner = new Context();
  const factory = new ConfluentKafkaClientFactory();
  await admin.connect();
  await producer.connect();
  try {
    await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
    for (let index = 0; index < 6; ++index)
      await producer.send({ topic, messages: [{ partition: 0, value: String(index) }] });
    for (const position of [4n, 1n, 5n]) {
      let calls = 0;
      const consumer = factory.consumer(brokers.split(","), `${topic}-group`, 5000, {
        context: owner, restoring: true,
        restore: () => { ++calls; return new Map([[0, position]]); }
      });
      let first: { record: KafkaRecord; control: KafkaConsumerControl } | undefined;
      let acknowledged = false;
      let failure: unknown;
      try {
        await consumer.connect();
        await consumer.subscribe(topic);
        const run = consumer.run(1, async (record, control) => {
          first ??= { record, control };
          await control.commit(record);
          acknowledged = true;
        });
        void run.catch((error: unknown) => { failure = error; });
        await until(() => acknowledged || failure !== undefined);
        assert.equal(failure, undefined);
        assert.equal(first?.record.offset, position);
        assert.equal(Buffer.from(first!.record.value).toString(), String(position));
        assert.ok(calls >= 1);
      } finally { await close(consumer, owner); }
      assert.ok(first?.control.signal.aborted);
      await assert.rejects(first!.control.commit(first!.record), /revoked assignment/u);
    }
    for (const checkpoint of [new Map<number, bigint>(), new Map([[0, 10000n]]), new Map([[0, -1n]]), new Map([[0, BigInt(Number.MAX_SAFE_INTEGER) + 1n]])]) {
      const consumer = factory.consumer(brokers.split(","), `${topic}-${randomUUID()}`, 5000, {
        context: owner, restoring: true, restore: () => checkpoint
      });
      let delivered = 0;
      try {
        await consumer.connect();
        await consumer.subscribe(topic);
        await assert.rejects(consumer.run(1, async () => { ++delivered; }), KafkaCheckpointError);
        assert.equal(delivered, 0);
      } finally { await close(consumer, owner); }
    }
  } finally { await producer.disconnect(); await admin.disconnect(); }
});

await test("two native consumers restore assignment and reject a previous owner's ACK", options, async () => {
  assert.ok(brokers);
  const module = createRequire(import.meta.url)("@confluentinc/kafka-javascript") as typeof ConfluentKafka;
  const kafka = new module.KafkaJS.Kafka({ kafkaJS: { brokers: brokers.split(",") } });
  const admin = kafka.admin();
  const producer = kafka.producer();
  const topic = `sa-ts-rebalance-${randomUUID()}`;
  const owner = new Context();
  const factory = new ConfluentKafkaClientFactory();
  const restore = (): CheckpointOffsets => new Map([[0, 1n], [1, 1n]]);
  const first = factory.consumer(brokers.split(","), topic, 5000, { context: owner, restoring: true, restore });
  const second = factory.consumer(brokers.split(","), topic, 5000, { context: owner, restoring: true, restore });
  const old = new Map<number, { record: KafkaRecord; control: KafkaConsumerControl }>();
  const received: KafkaRecord[] = [];
  const failures: unknown[] = [];
  await admin.connect();
  await producer.connect();
  try {
    await admin.createTopics({ topics: [{ topic, numPartitions: 2, replicationFactor: 1 }] });
    for (const partition of [0, 1]) {
      await producer.send({ topic, messages: [
        { partition, value: "before-image" }, { partition, value: "after-image" }
      ] });
    }
    await first.connect(); await first.subscribe(topic);
    void first.run(2, async (record, control) => {
      old.set(record.partition, { record, control });
      control.mark(record, "");
    }).catch((error: unknown) => { failures.push(error); });
    await until(() => old.size === 2 || failures.length > 0);
    assert.equal(failures.length, 0);
    await second.connect(); await second.subscribe(topic);
    void second.run(2, async (record, control) => {
      received.push(record); control.mark(record, "");
    }).catch((error: unknown) => { failures.push(error); });
    await until(() => received.length > 0 || failures.length > 0);
    assert.equal(failures.length, 0);
    for (const record of received) {
      assert.equal(record.offset, 1n);
      assert.equal(Buffer.from(record.value).toString(), "after-image");
      const prior = old.get(record.partition)!;
      assert.ok(prior.control.signal.aborted);
      await assert.rejects(prior.control.commit(prior.record), /revoked assignment/u);
    }
  } finally {
    await close(first, owner); await close(second, owner);
    await producer.disconnect(); await admin.disconnect();
  }
});
