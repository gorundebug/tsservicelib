import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { randomUUID } from "node:crypto";
import {
  Session,
  ensureResources,
  nativeFailure,
  snapshot
} from "../../dist/internal/pulsar/client.js";
import { Context, MessageContext } from "../../dist/runtime/index.js";
const url = process.env.PULSAR_TEST_URL;
const admin = process.env.PULSAR_TEST_ADMIN_URL;
const options = { skip: !url || !admin, timeout: 15000 };
const namespace = "public/ts-pulsar-" + randomUUID();
const topics = new Set(),
  sessions = new Set();
const cfg = {
  id: 11,
  name: "pulsar",
  type: 11,
  implementation: "pulsar-client-node",
  properties: {},
  servers: [url],
  adminUrl: admin,
  connectTimeoutMs: 1000,
  operationTimeoutMs: 1500
};
async function request(method, path, body) {
  const r = await fetch(admin + path, {
    method,
    headers: { "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(5000)
  });
  if (r.status === 404) return undefined;
  assert.ok(r.ok, `${method} ${path} HTTP${r.status}`);
  const text = await r.text();
  return text ? JSON.parse(text) : undefined;
}
function endpoint(source = false, patch = {}) {
  const topic = `persistent://${namespace}/t-${randomUUID()}`;
  topics.add(topic);
  return {
    id: source ? 100 : 101,
    name: "events",
    idDataConnector: 11,
    enabled: true,
    properties: {},
    topic,
    ...(source ? { subscriptionName: "workers" } : {}),
    ...patch
  };
}
function session(ep, source, config = cfg) {
  const s = new Session(config, ep, source, () => {});
  sessions.add(s);
  return s;
}
const owner = () => new Context().bounded(5000);
const message = () => new MessageContext().bounded(5000);
before(async () => {
  if (options.skip) return;
  await request("PUT", `/admin/v2/namespaces/${namespace}`, {});
  await request("POST", `/admin/v2/namespaces/${namespace}/autoTopicCreation`, {
    allowAutoTopicCreation: false
  });
  await request("POST", `/admin/v2/namespaces/${namespace}/autoSubscriptionCreation`, {
    allowAutoSubscriptionCreation: false
  });
});
after(async () => {
  if (options.skip) return;
  for (const s of sessions) await s.close(owner()).catch(() => {});
  for (const topic of topics) {
    const path = "/admin/v2/persistent/" + topic.slice("persistent://".length);
    const metadata = await request("GET", path + "/partitions?checkAllowAutoCreation=false");
    if (metadata)
      await request("DELETE", path + (metadata.partitions ? "/partitions" : "") + "?force=true");
  }
  await request("DELETE", `/admin/v2/namespaces/${namespace}`);
});
for (const partitions of [undefined, 2])
  test(`native resource opt-ins and shape ${partitions ?? 0}`, options, async () => {
    const ep = endpoint(true);
    await assert.rejects(ensureResources(cfg, ep, true, owner()), /404/);
    ep.createTopic = true;
    if (partitions) ep.partitions = partitions;
    await assert.rejects(ensureResources(cfg, ep, true, owner()), /subscription missing/);
    ep.createSubscription = true;
    await ensureResources(cfg, ep, true, owner());
    await ensureResources(cfg, { ...ep, createSubscription: false }, true, owner());
    await assert.rejects(
      ensureResources(cfg, { ...ep, partitions: partitions ? 3 : 2 }, true, owner()),
      /partition count differs/
    );
  });
test("native binary, metadata, ACK/NACK and durable replacement", options, async () => {
  const ep = endpoint(true, {
    createTopic: true,
    createSubscription: true,
    negativeAckRedeliveryDelayMs: 100
  });
  const input = session(ep, true);
  await input.start(owner());
  const output = session(endpoint(false, { topic: ep.topic }), false);
  await output.start(owner());
  const bytes = Buffer.from([0, 255, 1]),
    context = message().withStreamId("correlated");
  const copy = snapshot(
    { data: bytes, properties: { caller: "retained" }, partitionKey: "key", eventTimestamp: 1234 },
    context
  );
  const result = await output.publish(copy.message, context);
  assert.equal(result.outcome, "Accepted");
  let received = await input.receive(owner());
  assert.deepEqual(received.getData(), bytes);
  assert.deepEqual(received.getMessageId().serialize(), result.id.serialize());
  assert.equal(received.getPartitionKey(), "key");
  assert.equal(received.getEventTimestamp(), 1234);
  assert.equal(received.getProperties()["x-stream-id"], "correlated");
  await input.settle(received, input.epoch, owner(), true);
  received = await input.receive(owner());
  await input.close(owner());
  const replacement = session(ep, true);
  await replacement.start(owner());
  const pending = await replacement.receive(owner());
  assert.deepEqual(pending.getMessageId().serialize(), result.id.serialize());
  await replacement.settle(pending, replacement.epoch, owner());
  await assert.rejects(input.settle(received, input.epoch, owner()), /scope ended/);
  const cancel = new AbortController();
  const waiting = replacement.receive(new Context().withExternalCancellation(cancel.signal));
  cancel.abort(new Error("stop wait"));
  await assert.rejects(waiting, /stop wait/);
  // A cancelled local receive retains one native request; next receive reuses it.
  const next = await output.publish({ data: Buffer.from("next") }, message());
  assert.equal(next.outcome, "Accepted");
  assert.equal((await replacement.receive(owner())).getData().toString(), "next");
  await replacement.close(owner());
  await output.close(owner());
});
for (const mode of ["Shared", "Exclusive", "Failover"])
  test(`native ${mode} Latest and cursor reuse`, options, async () => {
    const ep = endpoint(false, { createTopic: true });
    const output = session(ep, false);
    await output.start(owner());
    await output.publish({ data: Buffer.from("old") }, message());
    const sourceEp = endpoint(true, {
      topic: ep.topic,
      subscriptionType: mode,
      createSubscription: true,
      subscriptionInitialPosition: "Latest"
    });
    const source = session(sourceEp, true);
    await source.start(owner());
    await output.publish({ data: Buffer.from("new") }, message());
    const received = await source.receive(owner());
    assert.equal(received.getData().toString(), "new");
    await source.close(owner());
    const next = session(sourceEp, true);
    await next.start(owner());
    const pending = await next.receive(owner());
    assert.deepEqual(pending.getMessageId().serialize(), received.getMessageId().serialize());
    await next.settle(pending, next.epoch, owner());
    await next.close(owner());
    await output.close(owner());
  });
test(
  "native source role, canceled context and payload validation are NotSent",
  options,
  async () => {
    const ep = endpoint(false, { createTopic: true });
    const output = session(ep, false);
    await output.start(owner());
    assert.equal((await output.publish({ data: "bad" }, message())).outcome, "NotSent");
    const abort = new AbortController();
    const reason = new Error("pre-handoff");
    abort.abort(reason);
    const rejected = await output.publish(
      { data: Buffer.from("none") },
      message().withExternalCancellation(abort.signal)
    );
    assert.equal(rejected.outcome, "NotSent");
    assert.equal(rejected.error, reason);
    await output.close(owner());
    assert.equal(
      (await output.publish({ data: Buffer.from("closed") }, message())).outcome,
      "NotSent"
    );
  }
);
for (const [text, outcome] of [
  ["ProducerQueueIsFull", "NotSent"],
  ["MessageTooBig", "NotSent"],
  ["AuthorizationError", "Rejected"],
  ["TopicTerminated", "Rejected"],
  ["AlreadyClosed", "Unknown"],
  ["Timeout", "Unknown"],
  ["NotConnected", "Unknown"]
])
  test(`versioned native outcome ${text}`, () => {
    const error = new Error("Failed to send message: " + text);
    const result = nativeFailure(error);
    assert.equal(result.outcome, outcome);
    assert.equal(result.error, error);
  });
test("unknown text never becomes confirmed rejection", () => {
  assert.equal(nativeFailure(new Error("some AuthorizationError substring")).outcome, "Unknown");
});

test(
  "receipt loss stays Unknown; native reconnect resumes without adapter retries",
  options,
  async (t) => {
    const { PulsarProxy } = await import("./pulsar-proxy.mjs");
    const proxy = await new PulsarProxy(url).start();
    t.after(() => proxy.close());
    const ep = endpoint(true, { createTopic: true, createSubscription: true });
    const input = session(ep, true);
    await input.start(owner());
    const output = session(endpoint(false, { topic: ep.topic }), false, {
      ...cfg,
      servers: [proxy.url]
    });
    await output.start(owner());
    proxy.dropReceipts = true;
    const result = await output.publish({ data: Buffer.from("ambiguous") }, message().bounded(100));
    assert.equal(result.outcome, "Unknown");
    const received = await input.receive(owner());
    assert.equal(received.getData().toString(), "ambiguous");
    await input.settle(received, input.epoch, owner());
    assert.equal(proxy.sends, 1);
    proxy.dropReceipts = false;
    proxy.disconnect();
    const next = await output.publish({ data: Buffer.from("resumed") }, message());
    assert.equal(next.outcome, "Accepted");
    let resumed;
    for (let i = 0; i < 3; i++) {
      resumed = await input.receive(owner());
      await input.settle(resumed, input.epoch, owner());
      if (resumed.getData().toString() === "resumed") break;
    }
    assert.equal(resumed.getData().toString(), "resumed");
    await output.close(owner());
    await input.close(owner());
  }
);

test("source pending receive closes promptly and stale settlement rejects", options, async () => {
  const input = session(endpoint(true, { createTopic: true, createSubscription: true }), true);
  await input.start(owner());
  assert.equal(
    (await input.publish({ data: Buffer.from("wrong-role") }, message())).outcome,
    "NotSent"
  );
  const pending = input.receive(owner());
  const rejected = assert.rejects(pending);
  const started = performance.now();
  await input.close(owner());
  await rejected;
  assert.ok(performance.now() - started < 1000);
});

for (const mutual of [false, true])
  test(`native TLS verified with client identity ${mutual}`, options, async (t) => {
    const { PulsarProxy } = await import("./pulsar-proxy.mjs"),
      { certificates, adminProxy } = await import("./pulsar-tls.mjs");
    const certs = await certificates();
    t.after(() => certs.close());
    const ssl = await certs.options(mutual);
    const proxy = await new PulsarProxy(url, ssl).start();
    t.after(() => proxy.close());
    const rest = await adminProxy(admin, await certs.options(false));
    t.after(() => rest.close());
    const ep = endpoint(false, { createTopic: true });
    const secure = {
      ...cfg,
      servers: [proxy.url],
      adminUrl: rest.url,
      tlsCaFile: certs.ca,
      ...(mutual ? { tlsCertFile: certs.cert, tlsKeyFile: certs.key } : {})
    };
    const output = session(ep, false, secure);
    await output.start(owner());
    assert.equal(
      (await output.publish({ data: Buffer.from("tls") }, message())).outcome,
      "Accepted"
    );
    await output.close(owner());
    // Reject untrusted roots on HTTPS administration, independently of native transport.
    await assert.rejects(session(ep, false, { ...secure, tlsCaFile: certs.wrong }).start(owner()));
    // Bypass only HTTPS in the fixture to exercise native TLS validation itself.
    const nativeOnly = { ...secure };
    if (mutual)
      await assert.rejects(
        session(ep, false, { ...nativeOnly, tlsCertFile: undefined, tlsKeyFile: undefined }).start(
          owner()
        )
      );

    const wrong = await new PulsarProxy(url, await certs.options(false, true)).start();
    t.after(() => wrong.close());
    await assert.rejects(
      session(ep, false, { ...nativeOnly, servers: [wrong.url] }).start(owner())
    );
    await assert.rejects(
      session(ep, false, { ...nativeOnly, servers: [wrong.url], tlsCaFile: certs.bundle }).start(
        owner()
      )
    );
  });

test(
  "cancelled waits retain native byte admission until native sends settle",
  options,
  async (t) => {
    const { PulsarProxy } = await import("./pulsar-proxy.mjs");
    const proxy = await new PulsarProxy(url).start();
    t.after(() => proxy.close());
    const ep = endpoint(false, { createTopic: true, publisherMaxInFlight: 32 });
    const output = session(ep, false, { ...cfg, servers: [proxy.url], operationTimeoutMs: 5000 });
    await output.start(owner());
    proxy.dropReceipts = true;
    // Each timeout releases the caller, but the native client still owns the payload.
    for (let i = 0; i < 8; i++)
      assert.equal(
        (await output.publish({ data: Buffer.alloc(1024 * 1024 - 128) }, message().bounded(30)))
          .outcome,
        "Unknown"
      );
    const count = proxy.sends;
    const rejected = await output.publish(
      { data: Buffer.alloc(1024 * 1024 - 128) },
      message().bounded(30)
    );
    assert.equal(rejected.outcome, "NotSent");
    assert.match(rejected.error.message, /byte admission/);
    assert.equal(proxy.sends, count);
    await output.close(owner());
  }
);

test(
  "startup deadline owns late native creation and bounded repeated close",
  options,
  async (t) => {
    const net = await import("node:net");
    const { once } = await import("node:events");
    const sockets = new Set();
    const server = net.createServer((s) => {
      sockets.add(s);
      s.on("error", () => {});
      s.on("close", () => sockets.delete(s));
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    t.after(async () => {
      for (const s of sockets) s.destroy();
      await new Promise((resolve) => server.close(resolve));
    });
    const output = session(endpoint(false, { createTopic: true }), false, {
      ...cfg,
      servers: [`pulsar://127.0.0.1:${server.address().port}`],
      connectTimeoutMs: 100,
      operationTimeoutMs: 1000
    });
    const start = performance.now();
    await assert.rejects(output.start(new Context().bounded(100)), /deadline/);
    assert.ok(performance.now() - start < 700);
    await output.close(new Context().bounded(3000));
    assert.equal(output.isReady(), false);
    assert.equal(
      (await output.publish({ data: Buffer.from("closed") }, message())).outcome,
      "NotSent"
    );
  }
);

test("explicit namespace policies are required and never silently changed", options, async () => {
  const ep = endpoint(true, { createTopic: true, createSubscription: true });
  for (const [policy, field] of [
    ["autoTopicCreation", "allowAutoTopicCreation"],
    ["autoSubscriptionCreation", "allowAutoSubscriptionCreation"]
  ]) {
    const path = `/admin/v2/namespaces/${namespace}/${policy}`;
    await request("POST", path, {
      [field]: true,
      ...(policy === "autoTopicCreation" ? { topicType: "non-partitioned" } : {})
    });
    try {
      await assert.rejects(ensureResources(cfg, ep, true, owner()), /explicitly disable/);
      assert.equal((await request("GET", path))[field], true);
    } finally {
      await request("POST", path, { [field]: false });
    }
  }
});

test("real terminated topic produces a definitive rejected native outcome", options, async () => {
  const ep = endpoint(false, { createTopic: true });
  const output = session(ep, false);
  await output.start(owner());
  await request(
    "POST",
    "/admin/v2/persistent/" + ep.topic.slice("persistent://".length) + "/terminate"
  );
  const result = await output.publish({ data: Buffer.from("terminated") }, message());
  assert.equal(result.outcome, "Rejected");
  assert.match(result.error.message, /TopicTerminated/);
  await output.close(owner());
});
