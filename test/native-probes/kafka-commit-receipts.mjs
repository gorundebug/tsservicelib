// Exercise the real SA native adapter with controlled official-client callbacks.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { ConfluentKafkaClientFactory } from "../../dist/datasource/kafka/index.js";
import { Context } from "../../dist/runtime/index.js";
const require = createRequire(import.meta.url);
const modulePath = require.resolve("@confluentinc/kafka-javascript");
// This isolated probe process replaces only the native module boundary.
const native = {
  KafkaJS: { ErrorCodes: { ERR__ASSIGN_PARTITIONS: -175, ERR__REVOKE_PARTITIONS: -174 } },
  KafkaConsumer: function () {}
};
require.cache[modulePath] = { id: modulePath, filename: modulePath, loaded: true, exports: native };
let instance;
class NativeConsumer {
  constructor(options) {
    instance = this;
    this.options = options;
    this.records = [];
    this.sent = [];
    this.assigned = false;
  }
  on() {
    return this;
  }
  setDefaultConsumeTimeout() {}
  connect(_, done) {
    done(null);
  }
  subscribe() {
    this.options.rebalance_cb(
      { code: native.KafkaJS.ErrorCodes.ERR__ASSIGN_PARTITIONS },
      [0, 1].map((partition) => ({ topic: "events", partition }))
    );
  }
  assign() {
    this.assigned = true;
  }
  unassign() {
    this.assigned = false;
  }
  pause() {}
  resume() {}
  seek(_, __, done) {
    done(null);
  }
  offsetsStore() {}
  commit(offset) {
    this.sent.push(offset);
  }
  consume(_, done) {
    setTimeout(() => done(null, this.records.splice(0)), 2);
  }
  isConnected() {
    return true;
  }
  disconnect(done) {
    done(null);
  }
}
async function until(check) {
  for (let n = 0; n < 500; ++n) {
    if (check()) return;
    await delay(2);
  }
  assert.fail("callback condition timed out");
}
async function harness(t) {
  t.mock.method(native, "KafkaConsumer", function (options) {
    return new NativeConsumer(options);
  });
  const owner = new Context();
  const consumer = new ConfluentKafkaClientFactory().consumer(["unused"], "group", 1000, {
    context: owner,
    restoring: false,
    restore: () => null
  });
  await consumer.connect();
  await consumer.subscribe("events");
  await until(() => instance.assigned);
  instance.records.push(
    ...[0, 1].map((partition) => ({
      topic: "events",
      partition,
      offset: 3,
      value: Buffer.from("value"),
      key: null,
      headers: []
    }))
  );
  const handles = new Map();
  const run = consumer.run(2, async (record, control) => {
    handles.set(record.partition, { record, control });
  });
  t.after(async () => {
    await consumer.stop(owner.bounded(1000));
    await run;
    await consumer.disconnect();
  });
  await until(() => handles.size === 2);
  return { handles, client: instance };
}
await test("scoped commit error cannot reject an unrelated partition", async (t) => {
  const { handles, client } = await harness(t);
  const a = handles.get(0),
    b = handles.get(1);
  const first = a.control.commit(a.record);
  const failed = assert.rejects(first, /partition-zero/);
  let secondDone = false;
  const second = b.control.commit(b.record).then(() => {
    secondDone = true;
  });
  client.options.offset_commit_cb(new Error("partition-zero"), [
    { topic: "events", partition: 0, offset: 4 }
  ]);
  await failed;
  await delay(10);
  assert.equal(secondDone, false);
  client.options.offset_commit_cb(null, [{ topic: "events", partition: 1, offset: 4 }]);
  await second;
});
await test("receipt after revoke cannot succeed for an old handle", async (t) => {
  const { handles, client } = await harness(t);
  const a = handles.get(0);
  const pending = a.control.commit(a.record);
  const rejected = assert.rejects(pending, /revoked assignment/);
  client.options.rebalance_cb({ code: native.KafkaJS.ErrorCodes.ERR__REVOKE_PARTITIONS }, []);
  client.options.offset_commit_cb(null, [{ topic: "events", partition: 0, offset: 4 }]);
  await rejected;
});
await test("unrelated automatic receipts do not complete an explicit position", async (t) => {
  const { handles, client } = await harness(t);
  const a = handles.get(0);
  let done = false;
  const pending = a.control.commit(a.record).then(() => {
    done = true;
  });
  for (const offset of [2, 3, 5])
    client.options.offset_commit_cb(null, [{ topic: "events", partition: 0, offset }]);
  await delay(10);
  assert.equal(done, false);
  client.options.offset_commit_cb(null, [{ topic: "events", partition: 0, offset: 4 }]);
  await pending;
});
await test("same-position native receipt is an observation, not individual request identity", async (t) => {
  const { handles, client } = await harness(t);
  const a = handles.get(0);
  const first = a.control.commit(a.record),
    second = a.control.commit(a.record);
  assert.equal(client.sent.length, 2);
  client.options.offset_commit_cb(null, [{ topic: "events", partition: 0, offset: 4 }]);
  await Promise.all([first, second]);
});
